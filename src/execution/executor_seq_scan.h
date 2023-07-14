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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    /**
     * @description: 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        // select * from table
        // select id from grade where name = 'Data';
        // 表迭代器
        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end()) {
            // 得到当前 rid
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (cmp_conds(rec.get(), conds_, cols_)) {
                break;
            }
            scan_->next();
        }
    }

    /**
     * @description: 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void nextTuple() override {
        if (scan_->is_end()) {
            return;
        }
        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (cmp_conds(rec.get(), conds_, cols_)) {
                break;
            }
        }
    }

    /**
    * @description: 返回下一个满足扫描条件的记录
    *
    * @return std::unique_ptr<RmRecord>
    */
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
    static inline int compare(const char* a, const char* b, int col_len, ColType col_type) {
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
    bool cmp_conds(const RmRecord* rec, const std::vector<Condition>& conds,  const std::vector<ColMeta>& rec_cols) {
        return std::all_of(conds.begin(), conds.end(), [&](const Condition &cond) {
            return cmp_cond(rec, cond, rec_cols);
        });
    }
};