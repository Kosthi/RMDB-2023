/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

static inline bool check_lock(Transaction* txn) {
    // 事务已经结束，不能再获取锁
    if (txn->get_state() == TransactionState::COMMITTED || txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    // 收缩状态不允许加锁
    if (txn->get_state() == TransactionState::SHRINKING) {
        // 抛出异常，在rmdb里abort
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    // 如果之前没加过锁，更新锁模式，开始2PL第一阶段
    if (txn->get_state() == TransactionState::DEFAULT) {
        txn->set_state(TransactionState::GROWING);
    }
    return true;
}

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);

    if (!check_lock(txn)) return false;

    // 得到记录的锁ID
    LockDataId lockDataId(tab_fd, rid, LockDataType::RECORD);
    // 如果锁表中不存在，使用分段构造构造map
    if (lock_table_.count(lockDataId) == 0) {
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lockDataId), std::forward_as_tuple());
    }
    // 得到锁ID所在的锁请求队列和队列上的所有锁请求
    auto& lockRequestQueue = lock_table_.at(lockDataId);
    auto& lockRequests = lockRequestQueue.request_queue_;

    // 事务上已经有这个记录的共享锁了，判断为加锁成功
    auto pos = std::find_if(lockRequests.begin(), lockRequests.end(), [&](const LockRequest& lockRequest) {
        return lockRequest.txn_id_ == txn->get_transaction_id();
    });
    // select 加IS和S锁，对于S锁的申请直接通过，而不是升级为S
    if (pos != lockRequests.end()) return true;

    // 如果其他事务持有X锁，则加锁失败(no-wait)
    if (lockRequestQueue.group_lock_mode_ == GroupLockMode::X ||
        lockRequestQueue.group_lock_mode_ == GroupLockMode::IX ||
        lockRequestQueue.group_lock_mode_ == GroupLockMode::SIX) {
        // no-wait!
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        // lockRequestQueue.cv_.wait(lock);
    }

    // 设置队列锁模式为共享锁
    lockRequestQueue.group_lock_mode_ = GroupLockMode::S;
    // 添加当前事务的锁请求到队列中
    lockRequestQueue.request_queue_.emplace_back(txn->get_transaction_id(), LockMode::SHARED);
    ++lockRequestQueue.shared_lock_num_;
    // 成功申请共享锁
    lockRequestQueue.request_queue_.back().granted_ = true;
    txn->get_lock_set()->emplace(lockDataId);
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);

    if (!check_lock(txn)) return false;

    // 得到记录的锁ID
    LockDataId lockDataId(tab_fd, rid, LockDataType::RECORD);

    // 如果锁表中不存在，使用分段构造构造map
    if (lock_table_.count(lockDataId) == 0) {
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lockDataId), std::forward_as_tuple());
    }
    // 得到锁ID所在的锁请求队列和队列上的所有锁请求
    auto& lockRequestQueue = lock_table_.at(lockDataId);
    auto& lockRequests = lockRequestQueue.request_queue_;

    // 如果队列中已经有这个事务
    // 如果有排他锁了，判断为加锁成功，否则抛出异常，回滚
    auto pos = std::find_if(lockRequests.begin(), lockRequests.end(), [&](const LockRequest& lockRequest) {
        return lockRequest.txn_id_ == txn->get_transaction_id();
    });
    if (pos != lockRequests.end()) {
        // select后修改
        // 多次修改
        // 修改再select
        if (pos->lock_mode_ == LockMode::EXCLUSIVE) {
            return true;
        }
        // 如果事务要升级为写锁，该记录不能有其他事务在读
        else if ((pos->lock_mode_ == LockMode::INTENTION_SHARED || pos->lock_mode_ == LockMode::SHARED) && lockRequests.size() == 1) {
            pos->lock_mode_ = LockMode::EXCLUSIVE;
            lockRequestQueue.group_lock_mode_ = GroupLockMode::X;
            return true;
        }
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    // 如果其他事务持有其他锁，则加锁失败(no-wait)
    if (lockRequestQueue.group_lock_mode_ != GroupLockMode::NON_LOCK) {
        // no-wait!
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        // lockRequestQueue.cv_.wait(lock);
    }
    // 设置队列锁模式为排他锁
    lockRequestQueue.group_lock_mode_ = GroupLockMode::X;
    // 添加当前事务的锁请求到队列中
    lockRequestQueue.request_queue_.emplace_back(txn->get_transaction_id(), LockMode::EXCLUSIVE);
    // 成功申请排他锁
    lockRequestQueue.request_queue_.back().granted_ = true;
    txn->get_lock_set()->emplace(lockDataId);
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);

    if (!check_lock(txn)) return false;

    // 3. 创建锁请求对象
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    // 4. 如果锁表中还不存在该对象，则构造该对象
    if(!lock_table_.count(lock_data_id)) {
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lock_data_id), std::forward_as_tuple());
    }
    // 5. 构造lock request
    LockRequest request(txn->get_transaction_id(), LockMode::SHARED);
    auto &request_queue = lock_table_[lock_data_id];
    // 6. 查看是否该事务已经持有对应的锁
    for(auto &req : request_queue.request_queue_) {
        if(req.txn_id_ == txn->get_transaction_id()) {
            if(req.lock_mode_ == LockMode::EXCLUSIVE || req.lock_mode_ == LockMode::S_IX || req.lock_mode_ == LockMode::SHARED) {
                // 6.1 该事务已经持有S锁或者更高级的锁，例如X,S_IX,S
                return true;
            }else if(req.lock_mode_ == LockMode::INTENTION_SHARED){
                // 6.2 该事务持有了IS锁，需要升级为S锁
                if(request_queue.group_lock_mode_ == GroupLockMode::IS || request_queue.group_lock_mode_ == GroupLockMode::S) {
                    req.lock_mode_ = LockMode::SHARED;
                    request_queue.group_lock_mode_ = GroupLockMode::S;
                    return true;
                }else {
                    throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                }
            }else {
                // 6.3 该事务持有了IX锁，需要升级为S_IX锁，升级条件为当前仅有一个IX锁
                int num_ix = 0;
                for(auto const &req2 : request_queue.request_queue_) {
                    if(req2.lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
                        num_ix++;
                    }
                }
                if(num_ix == 1) {
                    req.lock_mode_ = LockMode::S_IX;
                    request_queue.group_lock_mode_ = GroupLockMode::SIX;
                    return true;
                }else {
                    throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                }
            }
        }
    }

    // 7. 事务没有持有该锁，需要申请S锁，申请条件group lock为S,IS,NON_LOCK
    if(request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK ||
       request_queue.group_lock_mode_ == GroupLockMode::S ||
       request_queue.group_lock_mode_ == GroupLockMode::IS) {

        txn->get_lock_set()->emplace(lock_data_id);

        request.granted_ = true;

        request_queue.group_lock_mode_ = GroupLockMode::S;
        request_queue.request_queue_.emplace_back(request);
        return true;
    }else {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);

    if (!check_lock(txn)) return false;

    // 3. 创建锁请求对象
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    // 4. 如果锁表中还不存在该对象，则构造该对象
    if(!lock_table_.count(lock_data_id)) {
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lock_data_id), std::forward_as_tuple());
    }
    // 5. 构造lock request
    LockRequest request(txn->get_transaction_id(), LockMode::EXCLUSIVE);
    auto &request_queue = lock_table_[lock_data_id];
    // 6. 查看是否该事务已经持有对应的锁
    for(auto &req : request_queue.request_queue_) {
        if(req.txn_id_ == txn->get_transaction_id()) {
            if(req.lock_mode_ == LockMode::EXCLUSIVE) {
                // 6.1 该事务已经持有X锁
                return true;
            }else {
                // 6.2 该事务持有了某种锁，需要升级为X锁，升级条件为当前仅有该事务持有锁
                if(request_queue.request_queue_.size() == 1) {
                    req.lock_mode_ = LockMode::EXCLUSIVE;
                    request_queue.group_lock_mode_ = GroupLockMode::X;
                    return true;
                }else {
                    throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                }
            }
        }
    }

    // 7. 事务没有持有X锁，需要申请，申请条件NON_LOCK
    if(request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
        txn->get_lock_set()->emplace(lock_data_id);
        request.granted_ = true;
        request_queue.group_lock_mode_ = GroupLockMode::X;
        request_queue.request_queue_.emplace_back(request);
        return true;
    }else {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);

    if (!check_lock(txn)) return false;

    // 得到记录所在锁的请求队列
    LockDataId lockDataId(tab_fd, LockDataType::TABLE);

    // 如果锁表中不存在，使用分段构造构造map
    if (lock_table_.count(lockDataId) == 0) {
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lockDataId), std::forward_as_tuple());
    }
    // 得到锁ID所在的锁请求队列和队列上的所有锁请求
    auto& lockRequestQueue = lock_table_.at(lockDataId);
    auto& lockRequests = lockRequestQueue.request_queue_;

    auto pos = std::find_if(lockRequests.begin(), lockRequests.end(), [&](const LockRequest& lockRequest) {
        return lockRequest.txn_id_ == txn->get_transaction_id();
    });
    // 如果队列中已经有这个事务
    // 没有比IS锁更低级，直接申请成功
    if (pos != lockRequests.end()) {
        return true;
    }

    // 如果其他事务持有排他锁，则加锁失败(no-wait)
    if (lockRequestQueue.group_lock_mode_ == GroupLockMode::X) {
        // no-wait!
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        // lockRequestQueue.cv_.wait(lock);
    }

    // 如果队列没有锁才设置队列锁模式为共享锁
    if (lockRequestQueue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
        lockRequestQueue.group_lock_mode_ = GroupLockMode::IS;
    }
    // 添加当前事务的锁请求到队列中
    lockRequestQueue.request_queue_.emplace_back(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
    lockRequestQueue.request_queue_.back().granted_ = true;
    // 成功申请共享锁
    txn->get_lock_set()->emplace(lockDataId);
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);

    if (!check_lock(txn)) return false;

    // 得到记录所在锁的请求队列
    LockDataId lockDataId(tab_fd, LockDataType::TABLE);

    // 如果锁表中不存在，使用分段构造构造map
    if (lock_table_.count(lockDataId) == 0) {
        lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(lockDataId), std::forward_as_tuple());
    }
    // 得到锁ID所在的锁请求队列和队列上的所有锁请求
    auto& lockRequestQueue = lock_table_.at(lockDataId);
    auto& lockRequests = lockRequestQueue.request_queue_;

    auto pos = std::find_if(lockRequests.begin(), lockRequests.end(), [&](const LockRequest& lockRequest) {
        return lockRequest.txn_id_ == txn->get_transaction_id();
    });
    // 如果队列中已经有这个事务
    // 注意对于同一事务内部的锁和不同事务之间的锁处理逻辑不同
    if (pos != lockRequests.end()) {
        // 如果已经有IX锁或更高级的锁，同一事务中多个写申请
        if (pos->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
            pos->lock_mode_ == LockMode::S_IX ||
            pos->lock_mode_ == LockMode::EXCLUSIVE) {
            return true;
        }
        // 如果事务有且持有唯一共享锁(where)，则升级为SIX
        // 否则有多个事务持有共享锁，无法为其中的一个事务申请写锁，因为可能会影响其他事务读出来修改后的数据
        else if (pos->lock_mode_ == LockMode::SHARED && lockRequestQueue.shared_lock_num_ == 1) {
            ++lockRequestQueue.IX_lock_num_;
            pos->lock_mode_ = LockMode::S_IX;
            lockRequestQueue.group_lock_mode_ = GroupLockMode::SIX;
            return true;
        }
        else if (pos->lock_mode_ == LockMode::INTENTION_SHARED && (
                lockRequestQueue.group_lock_mode_ == GroupLockMode::IS ||
                lockRequestQueue.group_lock_mode_ == GroupLockMode::IX)) {
            ++lockRequestQueue.IX_lock_num_;
            pos->lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
            lockRequestQueue.group_lock_mode_ = GroupLockMode::IX;
            return true;
        }
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }
    // 得到记录所在锁的请求队列
    // 如果其他事务持有共享(或SIX)锁和排它锁，则加锁失败(no-wait)
    if (lockRequestQueue.group_lock_mode_ == GroupLockMode::S ||
        lockRequestQueue.group_lock_mode_ == GroupLockMode::SIX ||
        lockRequestQueue.group_lock_mode_ == GroupLockMode::X) {
        // no-wait!
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        // lockRequestQueue.cv_.wait(lock);
    }
    // 设置队列锁模式为意向排他锁
    lockRequestQueue.group_lock_mode_ = GroupLockMode::IX;
    // 添加当前事务的锁请求到队列中
    lockRequestQueue.request_queue_.emplace_back(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
    ++lockRequestQueue.IX_lock_num_;
    // 成功申请意向排他锁
    lockRequestQueue.request_queue_.back().granted_ = true;
    txn->get_lock_set()->emplace(lockDataId);
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);

    // 事务已经结束，不能再释放锁
    if (txn->get_state() == TransactionState::COMMITTED || txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    // 之前没有获得锁
    if (txn->get_state() == TransactionState::DEFAULT) {
        // 抛出异常，在rmdb里abort
        // throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    // 如果之前加过锁，更新锁模式，开始2PL第二阶段
    if (txn->get_state() == TransactionState::GROWING) {
        txn->set_state(TransactionState::SHRINKING);
    }

    // 找不到锁请求队列
    if (lock_table_.find(lock_data_id) == lock_table_.end()) {
        return true;
    }

    // 得到锁ID所在的锁请求队列和队列上的所有锁请求
    auto& lockRequestQueue = lock_table_.at(lock_data_id);
    auto& lockRequests = lockRequestQueue.request_queue_;

    // 找到当前事务的锁请求并移除
    auto pos = std::find_if(lockRequests.begin(), lockRequests.end(), [&](const LockRequest& lockRequest) {
        return lockRequest.txn_id_ == txn->get_transaction_id();
    });
    // 找不到当前事务
    if (pos == lockRequests.end()) return true;
    if (pos->lock_mode_ == LockMode::SHARED || pos->lock_mode_ == LockMode::S_IX) {
        --lockRequestQueue.shared_lock_num_;
    }
    if (pos->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || pos->lock_mode_ == LockMode::S_IX) {
        --lockRequestQueue.IX_lock_num_;
    }
    lockRequests.erase(pos);

    // 更新锁队列的锁模式
    if (lockRequests.empty()) {
        lockRequestQueue.group_lock_mode_ = GroupLockMode::NON_LOCK;
        return true;
    }

//    LockMode new_mode = LockMode::INTENTION_SHARED;
//    for (auto& lockRequest : lockRequests) {
//        new_mode = static_cast<LockMode>(std::max(static_cast<int>(new_mode), static_cast<int>(lockRequest.lock_mode_)));
//    }
//    lockRequestQueue.group_lock_mode_ = static_cast<GroupLockMode>(static_cast<int>(new_mode) + 1);
    // 6. 更新request queue的元信息
    int IS_lock_num = 0, IX_lock_num = 0, S_lock_num = 0, SIX_lock_num = 0, X_lock_num = 0;
    for(auto const &req : lockRequests) {
        switch (req.lock_mode_)
        {
            case LockMode::INTENTION_SHARED: {
                IS_lock_num++;
                break;
            }
            case LockMode::INTENTION_EXCLUSIVE: {
                IX_lock_num++;
                break;
            }
            case LockMode::SHARED: {
                S_lock_num++;
                break;
            }
            case LockMode::EXCLUSIVE: {
                X_lock_num++;
                break;
            }
            case LockMode::S_IX: {
                SIX_lock_num++;
                break;
            }
            default:
                break;
        }
    }
    if(X_lock_num > 0) {
        lockRequestQueue.group_lock_mode_ = GroupLockMode::X;
    }else if(SIX_lock_num > 0) {
        lockRequestQueue.group_lock_mode_ = GroupLockMode::SIX;
    }else if(IX_lock_num > 0) {
        lockRequestQueue.group_lock_mode_ = GroupLockMode::IX;
    }else if(S_lock_num > 0) {
        lockRequestQueue.group_lock_mode_ = GroupLockMode::S;
    }else if(IS_lock_num > 0) {
        lockRequestQueue.group_lock_mode_ = GroupLockMode::IS;
    }else {
        lockRequestQueue.group_lock_mode_ = GroupLockMode::NON_LOCK;
    }
    return true;
}