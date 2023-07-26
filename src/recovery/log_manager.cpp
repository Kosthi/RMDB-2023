/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstring>
#include "log_manager.h"

/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
lsn_t LogManager::add_log_to_buffer(LogRecord* log_record) {
    latch_.lock();

    // 将日志记录序列化到缓冲区中
    char serialized_log[log_record->log_tot_len_];
    log_record->lsn_ = global_lsn_++;
    log_record->serialize(serialized_log);

    // 确保缓冲区有足够的空间容纳这个日志记录
    if (log_buffer_.is_full(log_record->log_tot_len_)) {
        // 如果缓冲区满了，就将缓冲区中的内容刷到磁盘，并清空缓冲区
        latch_.unlock();
        flush_log_to_disk();
        latch_.lock();
    }

    memcpy(log_buffer_.buffer_ + log_buffer_.offset_, serialized_log, log_record->log_tot_len_);
    log_buffer_.offset_ += log_record->log_tot_len_;

    latch_.unlock();
    return log_record->lsn_;
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk() {
    std::lock_guard<std::mutex> lock(latch_);
    disk_manager_->write_log(log_buffer_.buffer_, log_buffer_.offset_);
    memset(log_buffer_.buffer_, 0, sizeof(log_buffer_.buffer_));
    log_buffer_.offset_ = 0;
    persist_lsn_ = global_lsn_ - 1;
}

/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when timeout or the log buffer is full or buffer
 * pool manager wants to force flush (it only happens when the flushed page has
 * a larger LSN than persistent LSN)
 *
 * This thread runs forever until system shutdown/StopFlushThread
 */
void LogManager::RunFlushThread() {
//    if (enable_logging) {
//        return;
//    }
//    enable_logging = true;
//    flush_thread_ = new std::thread([&] {
//        while (enable_logging) {
//            std::unique_lock<std::mutex> lock(latch_);
//            // flush log to disk if log time out or log buffer is full
//            cv_.wait_for(lock, log_timeout, [&] { return need_flush_.load(); });
//        }
//    })
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {

}