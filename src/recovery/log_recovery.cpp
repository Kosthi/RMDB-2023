/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {
    uint32_t log_offset = 0;
    uint32_t buffer_offset = 0;
    char log_buffer[LOG_BUFFER_SIZE];
    int bytes_read = 0;
    while ((bytes_read = disk_manager_->read_log(log_buffer, LOG_BUFFER_SIZE, log_offset)) > 0) {
        buffer_offset = 0;
        while (buffer_offset < bytes_read) {
            auto log_type = *reinterpret_cast<const LogType*>(log_buffer + buffer_offset);
            switch (log_type) {
                case LogType::NEWPAGE: {
                    NewpageLogRecord newpageLogRecord;
                    newpageLogRecord.deserialize(log_buffer + buffer_offset);

                    active_txn_.emplace(newpageLogRecord.log_tid_, newpageLogRecord.lsn_);
                    lsn_mapping_.emplace(newpageLogRecord.lsn_, log_offset + buffer_offset);

                    // 因为自增分配策略，如果pageid大于filehdr里的已分配pageid，则说明该页可能未刷盘
                    // 也有可能filehdr没有刷盘，没有及时更新正确的页数，这样的情况下如果new的话会清空原来页面的数据，不可取
                    // 应该set_fd2pageno
                    // 判断是否刷过盘
                    auto rm = sm_manager_->fhs_.at(newpageLogRecord.table_name_).get();

                    PageId pageId{rm->GetFd(), newpageLogRecord.page_id_};
                    if (!disk_manager_->is_flushed(pageId.fd, pageId.page_no)) {
                        auto page = rm->create_new_page_handle(nullptr).page;
                        page->set_page_lsn(-1);
                        // 这里不能unpin，否则页面可能会被置换，导致fetch读空页面
                    }

                    buffer_offset += newpageLogRecord.log_tot_len_;
                    break;
                }
                case LogType::begin: {
                    BeginLogRecord beginLogRecord;
                    beginLogRecord.deserialize(log_buffer + buffer_offset);
                    // update ATT for undo.
                    active_txn_.emplace(beginLogRecord.log_tid_, beginLogRecord.lsn_);
                    lsn_mapping_.emplace(beginLogRecord.lsn_, log_offset + buffer_offset);

                    buffer_offset += beginLogRecord.log_tot_len_;
                    break;
                }
                case LogType::commit: {
                    CommitLogRecord commitLogRecord;
                    commitLogRecord.deserialize(log_buffer + buffer_offset);
                    active_txn_.erase(commitLogRecord.log_tid_);
                    lsn_mapping_.emplace(commitLogRecord.lsn_, log_offset + buffer_offset);

                    buffer_offset += commitLogRecord.log_tot_len_;
                    break;
                }
                case LogType::ABORT: {
                    AbortLogRecord abortLogRecord;
                    abortLogRecord.deserialize(log_buffer + buffer_offset);
                    active_txn_.erase(abortLogRecord.log_tid_);
                    lsn_mapping_.emplace(abortLogRecord.lsn_, log_offset + buffer_offset);

                    buffer_offset += abortLogRecord.log_tot_len_;
                    break;
                }
                case LogType::INSERT: {
                    InsertLogRecord insertLogRecord;
                    insertLogRecord.deserialize(log_buffer + buffer_offset);
                    active_txn_.emplace(insertLogRecord.log_tid_, insertLogRecord.lsn_);
                    lsn_mapping_.emplace(insertLogRecord.lsn_, log_offset + buffer_offset);

                    auto fm = sm_manager_->fhs_.at(insertLogRecord.table_name_).get();
                    // update DPT for redo.
                    PageId pageId{fm->GetFd(), insertLogRecord.rid_.page_no};
                    auto page = buffer_pool_manager_->fetch_page(pageId);
                    if (page->get_page_lsn() < insertLogRecord.lsn_) {
                        dirty_page_table_.emplace_back(insertLogRecord.lsn_);
                    }
                    buffer_pool_manager_->unpin_page(pageId, false);

                    buffer_offset += insertLogRecord.log_tot_len_;
                    break;
                }
                case LogType::DELETE: {
                    DeleteLogRecord deleteLogRecord;
                    deleteLogRecord.deserialize(log_buffer + buffer_offset);

                    active_txn_.emplace(deleteLogRecord.log_tid_, deleteLogRecord.lsn_);
                    lsn_mapping_.emplace(deleteLogRecord.lsn_, log_offset + buffer_offset);

                    auto fm = sm_manager_->fhs_.at(deleteLogRecord.table_name_).get();
                    // update DPT for redo.
                    PageId pageId{fm->GetFd(), deleteLogRecord.rid_.page_no};
                    auto page = buffer_pool_manager_->fetch_page(pageId);
                    if (page->get_page_lsn() < deleteLogRecord.lsn_) {
                        dirty_page_table_.emplace_back(deleteLogRecord.lsn_);
                    }
                    buffer_pool_manager_->unpin_page(pageId, false);

                    buffer_offset += deleteLogRecord.log_tot_len_;
                    break;
                }
                case LogType::UPDATE: {
                    UpdateLogRecord updateLogRecord;
                    updateLogRecord.deserialize(log_buffer + buffer_offset);

                    active_txn_.emplace(updateLogRecord.log_tid_, updateLogRecord.lsn_);
                    lsn_mapping_.emplace(updateLogRecord.lsn_, log_offset + buffer_offset);

                    auto fm = sm_manager_->fhs_.at(updateLogRecord.table_name_).get();
                    // update DPT for redo.
                    PageId pageId{fm->GetFd(), updateLogRecord.rid_.page_no};
                    auto page = buffer_pool_manager_->fetch_page(pageId);
                    if (page->get_page_lsn() < updateLogRecord.lsn_) {
                        dirty_page_table_.emplace_back(updateLogRecord.lsn_);
                    }
                    buffer_pool_manager_->unpin_page(pageId, false);

                    buffer_offset += updateLogRecord.log_tot_len_;
                    break;
                }
                default:
                    break;
            }
        }
        log_offset += buffer_offset;
    }
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {
    char log_buffer[LOG_BUFFER_SIZE];

    for (auto& lsn : dirty_page_table_) {
        auto log_offset = lsn_mapping_.at(lsn);
        disk_manager_->read_log(log_buffer, LOG_BUFFER_SIZE, log_offset);
        auto log_type = *reinterpret_cast<const LogType*>(log_buffer);
        switch (log_type) {
            case LogType::INSERT: {
                InsertLogRecord insertLogRecord;
                insertLogRecord.deserialize(log_buffer);

                auto fm = sm_manager_->fhs_.at(insertLogRecord.table_name_).get();
                fm->insert_record(insertLogRecord.rid_, insertLogRecord.insert_value_.data, nullptr);

                // 只有insert才会发生newpage
                // 因为已经写入数据 unpin两次消掉pin
                PageId pageId{fm->GetFd(), insertLogRecord.rid_.page_no};
                buffer_pool_manager_->fetch_page(pageId);
                // false还是true无所谓，减少几次指令罢了
                buffer_pool_manager_->unpin_page(pageId, false);
                buffer_pool_manager_->unpin_page(pageId, false);

                break;
            }
            case LogType::DELETE: {
                DeleteLogRecord deleteLogRecord;
                deleteLogRecord.deserialize(log_buffer);

                auto fm = sm_manager_->fhs_.at(deleteLogRecord.table_name_).get();
                fm->delete_record(deleteLogRecord.rid_, nullptr);

                break;
            }
            case LogType::UPDATE: {
                UpdateLogRecord updateLogRecord;
                updateLogRecord.deserialize(log_buffer);

                auto fm = sm_manager_->fhs_.at(updateLogRecord.table_name_).get();
                fm->update_record(updateLogRecord.rid_, updateLogRecord.update_value_.data, nullptr);

                break;
            }
            default:
                break;
        }
    }
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    char log_buffer[LOG_BUFFER_SIZE];

    for (auto& [txn_id, lsn] : active_txn_) {
        while (lsn != INVALID_LSN) {
            auto log_offset = lsn_mapping_.at(lsn);
            disk_manager_->read_log(log_buffer, LOG_BUFFER_SIZE, log_offset);
            auto log_type = *reinterpret_cast<const LogType*>(log_buffer);
            switch (log_type) {
                case LogType::NEWPAGE: {
                    NewpageLogRecord newpageLogRecord;
                    newpageLogRecord.deserialize(log_buffer);
                    lsn = newpageLogRecord.prev_lsn_;
                    break;
                }
                case LogType::begin: {
                    BeginLogRecord beginLogRecord;
                    beginLogRecord.deserialize(log_buffer);
                    lsn = beginLogRecord.prev_lsn_;
                    break;
                }
                case LogType::commit: {
                    CommitLogRecord commitLogRecord;
                    commitLogRecord.deserialize(log_buffer);
                    lsn = commitLogRecord.prev_lsn_;
                    break;
                }
                case LogType::ABORT: {
                    AbortLogRecord abortLogRecord;
                    abortLogRecord.deserialize(log_buffer);
                    lsn = abortLogRecord.prev_lsn_;
                    break;
                }
                case LogType::INSERT: {
                    InsertLogRecord insertLogRecord;
                    insertLogRecord.deserialize(log_buffer);

                    auto fm = sm_manager_->fhs_.at(insertLogRecord.table_name_).get();
                    fm->delete_record(insertLogRecord.rid_, nullptr);

                    lsn = insertLogRecord.prev_lsn_;
                    break;
                }
                case LogType::DELETE: {
                    DeleteLogRecord deleteLogRecord;
                    deleteLogRecord.deserialize(log_buffer);

                    auto fm = sm_manager_->fhs_.at(deleteLogRecord.table_name_).get();
                    fm->insert_record(deleteLogRecord.rid_, deleteLogRecord.delete_value_.data, nullptr);

                    lsn = deleteLogRecord.prev_lsn_;
                    break;
                }
                case LogType::UPDATE: {
                    UpdateLogRecord updateLogRecord;
                    updateLogRecord.deserialize(log_buffer);

                    auto fm = sm_manager_->fhs_.at(updateLogRecord.table_name_).get();
                    fm->update_record(updateLogRecord.rid_, updateLogRecord.old_value_.data, nullptr);

                    lsn = updateLogRecord.prev_lsn_;
                    break;
                }
                default:
                    break;
            }
        }
    }
}

void RecoveryManager::redo_index() {
    for (auto& [index_name, ih] : sm_manager_->ihs_) {
        sm_manager_->get_ix_manager()->close_index(ih.get());
        disk_manager_->destroy_file(index_name);

        // Remove the ".idx" suffix
        std::string name_without_suffix = index_name.substr(0, index_name.length() - 4);

        // Split the string using "_" as the delimiter
        std::string tab_name;
        std::vector<std::string> index_cols;
        std::istringstream iss(name_without_suffix);
        std::string token;
        std::getline(iss, token, '_');
        tab_name = token;
        while (std::getline(iss, token, '_')) {
            index_cols.push_back(token);
        }
        sm_manager_->create_index(tab_name, index_cols, nullptr);
    }
}