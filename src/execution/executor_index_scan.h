/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同
    // 优化后
    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
        std::reverse(fed_conds_.begin(), fed_conds_.end());
    }

    void beginTuple() override {
        // get b+tree
        auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_);
        auto ih = sm_manager_->ihs_[index_name].get();
        Iid lower = ih->leaf_begin(), upper = ih->leaf_end();
        // 构造索引key
        char* key = new char[index_meta_.col_tot_len + 4];
        char* last_eq_key = new char[index_meta_.col_tot_len + 4];
        int offset = 0;
        int idx = 0;
        for (; idx < index_meta_.cols.size(); ++idx) {
            memcpy(key + offset, conds_[idx].rhs_val.raw->data, index_meta_.cols[idx].len);
            offset += index_meta_.cols[idx].len;
            fed_conds_.pop_back();
            if (conds_[idx].op != OP_EQ || idx + 1 == conds_.size()) {
                idx++;
                break;
            }
        }
        idx--;
        memcpy(key + index_meta_.col_tot_len, &idx, 4);
        // 对于多列索引，需要考虑最后一个等号的位置 要么为idx，要么idx - 1
        if (idx > 0 && conds_[idx].op != OP_EQ) {
            memcpy(last_eq_key, key, index_meta_.col_tot_len);
            int tmp = idx - 1;
            memcpy(last_eq_key + index_meta_.col_tot_len, &tmp, 4);
        }
        // 只有最左叶子节点，需要考虑最小值大于等于小于key，其他节点都小于等于key
        if (conds_[idx].op == OP_EQ) {
            lower = ih->lower_bound(key);
            upper = ih->upper_bound_for_GT(key);
        }
        else if (conds_[idx].op == OP_GE) {
            lower = ih->lower_bound(key);
            // 对于多列索引，需要考虑新的上下限
            // 找满足第一个不满足多列等号条件的位置
            if (idx) upper = ih->upper_bound_for_GT(last_eq_key);
        }
        else if (conds_[idx].op == OP_LE) {
            // 找第一个大于key的位置，最终落在如果是中间或者最右叶子节点上的最小值必定小于等于key
            // 如果pos = size，则最大的也比key小 upper_bound = pos
            // 如果在最左叶子节点上，如果key_head <= key,upper_bound即使找到最左也是1
            // 如果key_head > key, upper_bound = 0
            // 对于中间和最右叶子节点，正常处理
            // 对于多列索引，需要考虑新的上下限
            if (idx) lower = ih->lower_bound(last_eq_key);
            upper = ih->upper_bound_for_GT(key);
        }
        else if (conds_[idx].op == OP_GT) {
            // 找第一个比key大的作为下限
            // 如果在最左叶子节点，最小值小于或等于或大于key
            // 最小值小于等于key，正常找；大于key pos = 0
            // 叶子节点的最大值如果小于等于key，则pos = size，即下一个叶子节点的第一个key
            // 最大值如果大于key，正常找
            // 如果在最右叶子节点 最大值小于等于key，则pos = size，找不到; 如果小于最大值，正常找
            // 如果在中间叶子节点 与最右叶子节点相同
            lower = ih->upper_bound_for_GT(key);
            // 对于多列索引，需要考虑新的上下限
            if (idx) upper = ih->upper_bound_for_GT(last_eq_key);
        }
        else if (conds_[idx].op == OP_LT) {
            // 找第一个大于等于key的作为上界
            // 如果在最左叶子节点 最小值大于等于小于key都有可能
            // 如果最小值大于等于key 则pos = 0；小于key，正常找
            // 如果最大值小于key pos = size；大于等于key，正常找
            // 最右叶子节点 最大值小于key pos = size; 大于等于key，正常找
            // 中间叶子节点 与最右叶子节点相同
            upper = ih->lower_bound(key);
            // 对于多列索引，需要考虑新的上下限
            if (idx) lower = ih->lower_bound(last_eq_key);
        }
        delete[] key;
        delete[] last_eq_key;
        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (cmp_conds(rec.get(), fed_conds_, cols_)) {
                break;
            }
            scan_->next();
        }
    }

    void nextTuple() override {
        scan_->next();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (cmp_conds(rec.get(), fed_conds_, cols_)) {
                break;
            }
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return scan_->is_end(); }

    size_t tupleLen() const override { return len_; }

    /**
    * @description: 比较数据数值
    *
    * @return std::unique_ptr<RmRecord>
    */
    static int compare(const char* a, const char* b, int col_len, ColType col_type) {
        switch (col_type) {
            case TYPE_INT: {
                int ai = *(int *) a;
                int bi = *(int *) b;
                return ai > bi ? 1 : ((ai < bi) ? -1 : 0);
            }
            case TYPE_FLOAT: {
                double af = *(double *) a;
                double bf = *(double *) b;
                return af > bf ? 1 : ((af < bf) ? -1 : 0);
            }
            case TYPE_BIGINT: {
                long long al = *(long long *) a;
                long long bl = *(long long *) b;
                return al > bl ? 1 : ((al < bl) ? -1 : 0);
            }
            case TYPE_STRING:
                return memcmp(a, b, col_len);
            case TYPE_DATETIME:
                return *(DateTime *)a == *(DateTime *)b;
            default:
                throw InternalError("Unexpected data type");
        }
    }

    // 判断是否满足单个谓词条件
    bool cmp_cond(const RmRecord* rec, const Condition& cond,  const std::vector<ColMeta>& rec_cols) {
        // 提取左值与右值的数据和类型
        auto lhs_col_meta = get_col(rec_cols, cond.lhs_col);
        char* lhs_data = rec->data + lhs_col_meta->offset;
        char* rhs_data;
        ColType rhs_type;

        // rhs is val
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            rhs_data = cond.rhs_val.raw->data;
        }
        else {
            // rhs is col
            auto rhs_col_meta = get_col(rec_cols, cond.rhs_col);
            rhs_type = rhs_col_meta->type;
            rhs_data = rec->data + rhs_col_meta->offset;
        }
        // 判断左右值数据类型是否相同
        if (lhs_col_meta->type != rhs_type) {
            throw IncompatibleTypeError(coltype2str(lhs_col_meta->type), coltype2str(rhs_type));
        }
        int cmp = compare(lhs_data, rhs_data, lhs_col_meta->len, rhs_type);
        switch (cond.op) {
            case OP_EQ: return cmp == 0;
            case OP_NE: return cmp != 0;
            case OP_LT: return cmp < 0;
            case OP_GT: return cmp > 0;
            case OP_LE: return cmp <= 0;
            case OP_GE: return cmp >= 0;
            default:
                throw InternalError("Unexpected op type");
        }
    }

    // 判断是否满足所有谓词条件
    bool cmp_conds(const RmRecord* rec, const std::vector<Condition>& conds, const std::vector<ColMeta>& rec_cols) {
        return std::all_of(conds.begin(), conds.end(), [&](const Condition &cond) {
            return cmp_cond(rec, cond, rec_cols);
        });
    }
};