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

class UpdateExecutor : public AbstractExecutor {
private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_; // 已经满足谓词条件
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        // 使用索引 更新表上的所有索引
        // 更新方式 在B+树上，删除旧的值，插入新值

        // 如果满足谓词条件的记录有多条，则更新的字段必须不是索引
        // 因为会对所有满足谓词条件的记录执行一样的更新操作
        int idx = -1;
        std::vector<RmRecord> old_records, new_records;
        std::vector<char*> old_datas[rids_.size()], new_datas[rids_.size()];
        std::vector<Rid> old_rids;

        for (size_t i = 0; i < rids_.size(); ++i) {
            auto old_record = fh_->get_record(rids_[i], context_);
            old_records.emplace_back(*old_record.get());
            RmRecord update_record = *old_record.get();
            for (auto &clause: set_clauses_) {
                auto lhs_col_meta = get_col(tab_.cols, clause.lhs);
                if (lhs_col_meta->type == TYPE_FLOAT && clause.rhs.type == TYPE_INT) {
                    clause.rhs.set_float(static_cast<double>(clause.rhs.int_val));
                    clause.rhs.raw = nullptr;
                    clause.rhs.init_raw(sizeof(double));
                } else if (lhs_col_meta->type == TYPE_INT && clause.rhs.type == TYPE_FLOAT) {
                    clause.rhs.set_int(static_cast<int>(clause.rhs.float_val));
                    clause.rhs.raw = nullptr;
                    clause.rhs.init_raw(sizeof(int));
                } else if (lhs_col_meta->type == TYPE_BIGINT && clause.rhs.type == TYPE_INT) {
                    clause.rhs.set_bigint(static_cast<long long>(clause.rhs.int_val));
                    clause.rhs.raw = nullptr;
                    clause.rhs.init_raw(sizeof(long long));
                } else if (lhs_col_meta->type == TYPE_INT && clause.rhs.type == TYPE_BIGINT) {
                    if (clause.rhs.bigint_val <= INT32_MAX && clause.rhs.bigint_val >= INT32_MIN) {
                        clause.rhs.set_int(static_cast<int>(clause.rhs.bigint_val));
                        clause.rhs.raw = nullptr;
                        clause.rhs.init_raw(sizeof(int));
                    }
                } else {
                    clause.rhs.raw = nullptr;
                    clause.rhs.init_raw(lhs_col_meta->len);
                }
                if (lhs_col_meta->type != clause.rhs.type) {
                    throw IncompatibleTypeError(coltype2str(lhs_col_meta->type), coltype2str(clause.rhs.type));
                }
                memcpy(update_record.data + lhs_col_meta->offset, clause.rhs.raw->data, clause.rhs.raw->size);
            }

            std::vector<Rid> rid_;
            for (auto &index: tab_.indexes) {
                // 进行唯一性检查
                auto ih = sm_manager_->ihs_.at(
                        sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char *update_data = new char[index.col_tot_len + 4];
                memcpy(update_data + index.col_tot_len, &idx, 4);
                int offset = 0;
                for (auto &col: index.cols) {
                    memcpy(update_data + offset, update_record.data + col.offset, col.len);
                    offset += col.len;
                }
                if (ih->get_value(update_data, &rid_, context_->txn_)) {
                    if (rid_.back() != rids_[i]) {
                        // 恢复
                        for (size_t j = 0; j < i; ++j) { // rid
                            for (size_t k = 0; k < tab_.indexes.size(); ++k) { // index
                                auto recover_ih = sm_manager_->ihs_.at(
                                        sm_manager_->get_ix_manager()->get_index_name(tab_name_, tab_.indexes[k].cols)).get();
                                recover_ih->delete_entry(new_datas[j][k], context_->txn_);
                                recover_ih->insert_entry(old_datas[j][k], rids_[j], context_->txn_);
                                delete[] new_datas[j][k];
                                delete[] old_datas[j][k];
                            }
                        }
                        throw InternalError("不满足唯一性约束！");
                    }
                }
                delete[] update_data;
            }
            // 通过检查，更新索引
            for (auto &index: tab_.indexes) {
                auto ih = sm_manager_->ihs_.at(
                        sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char *update_data = new char[index.col_tot_len + 4];
                char *old_data = new char[index.col_tot_len + 4];
                memcpy(update_data + index.col_tot_len, &idx, 4);
                memcpy(old_data + index.col_tot_len, &idx, 4);
                int offset = 0;
                for (auto &col: index.cols) {
                    memcpy(update_data + offset, update_record.data + col.offset, col.len);
                    memcpy(old_data + offset, old_record->data + col.offset, col.len);
                    offset += col.len;
                }
                assert(ih->delete_entry(old_data, context_->txn_));
                ih->insert_entry(update_data, rids_[i], context_->txn_);
                old_datas[i].emplace_back(old_data);
                new_datas[i].emplace_back(update_data);
            }
            // old_rids.emplace_back(rid_[i]);
            new_records.emplace_back(update_record);
        }
        // 更新记录
        for (size_t i = 0; i < rids_.size(); ++i) {
            fh_->update_record(rids_[i], new_records[i].data, context_);
        }
        return nullptr;
    }
//        if (tab_.indexes.empty()) return nullptr;
//        for (size_t i = 0; i < rids_.size(); ++i) {
//            auto old_record = old_records[i];
//            auto update_record = new_record[i];
//            // 更新索引
//            // 没必要? 优化 索引不包含set的任何字段
//            for (auto& index : tab_.indexes) {
//                // std::find_if(index.cols.begin(), index.cols.end(),)
//                char* old_delete_rec = new char[index.col_tot_len + 4];
//                char* new_insert_rec = new char[index.col_tot_len + 4];
//                memcpy(old_delete_rec + index.col_tot_len, &idx, 4);
//                memcpy(new_insert_rec + index.col_tot_len, &idx, 4);
//                auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
//                auto ih = sm_manager_->ihs_.at(index_name).get();
//                int offset = 0;
//                for (auto& index_col : index.cols) {
//                    memcpy(old_delete_rec + offset, old_record.data + index_col.offset, index_col.len);
//                    memcpy(new_insert_rec + offset, update_record.data + index_col.offset, index_col.len);
//                    offset += index_col.len;
//                }
//
//                if (!ih->delete_entry(old_delete_rec, context_->txn_)) {
//                    throw IndexEntryNotFoundError();
//                }
//                ih->insert_entry(new_insert_rec, rids_[i], context_->txn_);
//                delete[] old_delete_rec;
//                delete[] new_insert_rec;
//            }
//        }

    Rid &rid() override { return _abstract_rid; }
};