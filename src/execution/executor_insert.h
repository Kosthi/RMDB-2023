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

class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

   public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
        // 加表级写锁
        context_->lock_mgr_->lock_IX_on_table(context->txn_, fh_->GetFd());
    };

    std::unique_ptr<RmRecord> Next() override {
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            if (col.type == TYPE_BIGINT && val.type == TYPE_INT) {
                val.set_bigint(static_cast<long long>(val.int_val));
            }
            else if (col.type == TYPE_INT && val.type == TYPE_BIGINT) {
                if (val.bigint_val <= INT32_MAX && val.bigint_val >= INT32_MIN) {
                    val.set_int(static_cast<int>(val.bigint_val));
                }
            }
            // 题目9 日期向字符串的转换
            else if (col.type == TYPE_STRING && val.type == TYPE_DATETIME) {
                val.set_str(val.datetime_val.to_string());
            }
            if (col.type != val.type) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        // 唯一性检查
        int idx = -1;
        std::vector<Rid> rid;
        for (auto &index: tab_.indexes) {
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char *insert_data = new char[index.col_tot_len + 4];
            memcpy(insert_data + index.col_tot_len, &idx, 4);
            int offset = 0;
            for (auto &col: index.cols) {
                memcpy(insert_data + offset, rec.data + col.offset, col.len);
                offset += col.len;
            }
            if (ih->get_value(insert_data, &rid, context_->txn_)) {
                throw InternalError("Non-Unique Index!");
            }
            delete[] insert_data;
        }

        // Insert into record file
        rid_ = fh_->insert_record(rec.data, context_);
        // Insert into index
        for (auto& index : tab_.indexes) {
            auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
            auto ih = sm_manager_->ihs_.at(index_name).get();
            char* key = new char[index.col_tot_len + 4];
            memcpy(key + index.col_tot_len, &idx, 4);
            int offset = 0;
            for (size_t i = 0; i < index.col_num; ++i) {
                memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            ih->insert_entry(key, rid_, context_->txn_);
            RmRecord rm(index.col_tot_len + 4, key);
            WriteRecord* wr = new WriteRecord(WType::INSERT_TUPLE, rid_, rm, index_name);
            context_->txn_->append_write_record(wr);
            delete[] key;
        }
        RmRecord rm(rec.size, rec.data);

        // 更新log to log_buffer
        InsertLogRecord insertLogRecord(context_->txn_->get_transaction_id(), rm, rid_, tab_name_);
        insertLogRecord.prev_lsn_ = context_->txn_->get_prev_lsn();
        context_->txn_->set_prev_lsn(context_->log_mgr_->add_log_to_buffer(&insertLogRecord));
        // sm_manager_->get_bpm()->update_page_lsn(fh_->GetFd(), rid_.page_no, context_->txn_->get_prev_lsn());

        // 因为插入操作只有插入后才能得到rid信息，所以事务只需要存rid，在事务提交时不用再进行写操作
        WriteRecord* wr = new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_, rm);
        context_->txn_->append_write_record(wr);
        return nullptr;
    }
    Rid &rid() override { return rid_; }
};