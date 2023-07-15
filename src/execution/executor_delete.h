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

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        int idx = -1;
        for (auto& rid : rids_) {
            auto rm_record = fh_->get_record(rid, context_);
            char* prev_delete_rec = rm_record->data;
            for (auto& index : tab_.indexes) {
                char* delete_rec = new char[index.col_tot_len + 4];
                memcpy(delete_rec + index.col_tot_len, &idx, 4);
                auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
                auto ih = sm_manager_->ihs_.at(index_name).get();
                int offset = 0;
                for (auto& index_col : index.cols) {
                    memcpy(delete_rec + offset, prev_delete_rec + index_col.offset, index_col.len);
                    offset += index_col.len;
                }
                if (!ih->delete_entry(delete_rec, context_->txn_)) {
                    throw IndexEntryNotFoundError();
                }
                auto rm = RmRecord(index.col_tot_len + 4, delete_rec);
                WriteRecord* wr = new WriteRecord(WType::INSERT_TUPLE, rid, rm, index_name);
                context_->txn_->append_write_record(wr);
                delete[] delete_rec;
            }
            fh_->delete_record(rid, context_);
            WriteRecord* wr = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, *rm_record);
            context_->txn_->append_write_record(wr);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};