/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针

    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_++);
        txn->set_start_ts(next_timestamp_++);
    }
    std::lock_guard<std::mutex> lock(latch_);
    txn_map.emplace(txn->get_transaction_id(), txn);
    txn->set_state(TransactionState::GROWING);
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    // 什么叫作未提交
//    if (!txn->get_write_set()->empty()) {
//        for (auto& write_record : *txn->get_write_set()) {
//            auto file_handle = sm_manager_->fhs_.at(write_record->GetTableName()).get();
//            switch (write_record->GetWriteType()) {
//                case WType::INSERT_TUPLE: {
//                    file_handle->insert_record(write_record->GetRid(), write_record->GetRecord().data);
//                }   break;
//                case WType::DELETE_TUPLE: {
//                    file_handle->delete_record(write_record->GetRid(), nullptr);
//                }   break;
//                case WType::UPDATE_TUPLE: {
//                    file_handle->update_record(write_record->GetRid(), write_record->GetRecord().data, nullptr);
//                }   break;
//            }
//        }
//    }

    // 释放所有锁
    for (auto& lock : *txn->get_lock_set()) {
        lock_manager_->unlock(txn, lock);
    }

    // 释放事务相关资源，eg.锁集
    txn->get_write_set()->clear();
    txn->get_lock_set()->clear();
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();
    // 把事务日志刷入磁盘中
    log_manager->flush_log_to_disk();
    // 设置事务状态为已提交
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    // 回滚所有写操作
    auto q = txn->get_write_set();
    while (!q->empty()) {
        auto write_record = q->back();
        q->pop_back();
        // 回滚记录
        if (!write_record->GetTableName().empty()) {
            auto fh = sm_manager_->fhs_.at(write_record->GetTableName()).get();
            switch (write_record->GetWriteType()) {
                case WType::INSERT_TUPLE: {
                    fh->delete_record(write_record->GetRid(), nullptr);
                }
                    break;
                case WType::DELETE_TUPLE: {
                    fh->insert_record(write_record->GetRid(), write_record->GetRecord().data);
                }
                    break;
                case WType::UPDATE_TUPLE: {
                    fh->update_record(write_record->GetRid(), write_record->GetRecord().data, nullptr);
                }
                    break;
                default:
                    throw InternalError("Unsupported write type");
                    break;
            }
            delete write_record;
        }
        // 回滚索引
        else if (!write_record->GetIndexName().empty()) {
            auto ih = sm_manager_->ihs_.at(write_record->GetIndexName()).get();
            switch (write_record->GetWriteType()) {
                case WType::INSERT_TUPLE: {
                    ih->delete_entry(write_record->GetRecord().data, nullptr);
                }
                    break;
                case WType::DELETE_TUPLE: {
                    ih->insert_entry(write_record->GetRecord().data, write_record->GetRid(), nullptr);
                }
                    break;
                case WType::UPDATE_TUPLE: {
                    ih->delete_entry(write_record->GetUpdatedRecord().data, nullptr);
                    ih->insert_entry(write_record->GetOldRecord().data, write_record->GetRid(), nullptr);
                }
                    break;
                default:
                    throw InternalError("Unsupported write type");
                    break;
            }
            delete write_record;
        }
    }

    // 释放所有锁
    for (auto& lock : *txn->get_lock_set()) {
        lock_manager_->unlock(txn, lock);
    }

    // 释放事务相关资源，eg.锁集
    txn->get_write_set()->clear();
    txn->get_lock_set()->clear();
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();
    // 把事务日志刷入磁盘中
    log_manager->flush_log_to_disk();
    // 设置事务状态为已终止/回滚
    txn->set_state(TransactionState::ABORTED);
}