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
        // 暂时不用索引
//        std::vector<IxIndexHandle*> ihs(tab_.cols.size(), nullptr);
//        auto ix_manager = sm_manager_->get_ix_manager();
//        for (auto& clause : set_clauses_) {
//            auto lhs_col = tab_.get_col(clause.lhs.tab_name);
//            if (tab_.is_index(std::vector<std::string>(1, lhs_col->name))) {
//                size_t lhs_col_idx = lhs_col - tab_.cols.begin();
//                auto ix = sm_manager_->get_ix_manager();
//                ix->get_index_name()
//                ihs[lhs_col_idx] = sm_manager_.
//            }
//        }
        for (auto& rid : rids_) {
            auto record = fh_->get_record(rid, context_);
            RmRecord update_record = *record.get();
            for (auto& clause : set_clauses_) {
                auto lhs_col_meta = get_col(tab_.cols, clause.lhs);
                if (lhs_col_meta->type == TYPE_FLOAT && clause.rhs.type == TYPE_INT) {
                    clause.rhs.set_float(static_cast<double>(clause.rhs.int_val));
                    clause.rhs.raw = nullptr;
                    clause.rhs.init_raw(sizeof(double));
                }
//                else if (lhs_col_meta->type == TYPE_FLOAT && clause.rhs.type == TYPE_STRING) {
//                    clause.rhs.set_float(static_cast<int>(clause.rhs.float_val));
//                    clause.rhs.init_raw(sizeof(int));
//                }
                else if (lhs_col_meta->type == TYPE_INT && clause.rhs.type == TYPE_FLOAT) {
                    clause.rhs.set_int(static_cast<int>(clause.rhs.float_val));
                    clause.rhs.raw = nullptr;
                    clause.rhs.init_raw(sizeof(int));
                }
//                else if (lhs_col_meta->type == TYPE_INT && clause.rhs.type == TYPE_STRING) {
//                    clause.rhs.set_float(static_cast<int>(clause.rhs.float_val));
//                    clause.rhs.init_raw(sizeof(int));
//                }
//                else if (lhs_col_meta->type == TYPE_STRING && clause.rhs.type == TYPE_INT) {
//                    clause.rhs.set_str(std::to_string(clause.rhs.int_val));
//                    clause.rhs.init_raw(lhs_col_meta->len);
//                }
//                else if (lhs_col_meta->type == TYPE_STRING && clause.rhs.type == TYPE_FLOAT) {
//                    clause.rhs.set_str(std::to_string(clause.rhs.float_val));
//                    clause.rhs.init_raw(lhs_col_meta->len);
//                }
                else {
                    clause.rhs.raw = nullptr;
                    clause.rhs.init_raw(lhs_col_meta->len);
                }
                if (lhs_col_meta->type != clause.rhs.type) {
                    throw IncompatibleTypeError(coltype2str(lhs_col_meta->type), coltype2str(clause.rhs.type));
                }
                memcpy(update_record.data + lhs_col_meta->offset, clause.rhs.raw->data, clause.rhs.raw->size);
            }
            fh_->update_record(rid, update_record.data, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};