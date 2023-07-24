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
    if (pos != lockRequests.end()) {
        // 在获取S锁之前，先获取IS锁或更高级别的锁
        // 如果已经有S锁或更高级的锁，申请成功
        if (pos->lock_mode_ == LockMode::SHARED ||
            pos->lock_mode_ == LockMode::EXCLUSIVE ||
            pos->lock_mode_ == LockMode::S_IX) {
            return true;
        }
            // 事务有IS锁，升级S需要其他事务不持有X锁
        else if (pos->lock_mode_ == LockMode::INTENTION_SHARED && (
                lockRequestQueue.group_lock_mode_ == GroupLockMode::S ||
                lockRequestQueue.group_lock_mode_ == GroupLockMode::IS)) {
            pos->lock_mode_ = LockMode::SHARED;
            lockRequestQueue.group_lock_mode_ = GroupLockMode::S;
            ++lockRequestQueue.shared_lock_num_;
        }
            // 如果事务有IX锁，申请SIX需要其他事务不持有IX锁
        else if (pos->lock_mode_ == LockMode::INTENTION_EXCLUSIVE && lockRequestQueue.IX_lock_num_ == 1) {
            pos->lock_mode_ = LockMode::S_IX;
            lockRequestQueue.group_lock_mode_ = GroupLockMode::SIX;
            ++lockRequestQueue.shared_lock_num_;
        }
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    // 如果其他事务持有排他锁或者意向排它锁或SIX锁，则加锁失败(no-wait)
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
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
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
    if (pos != lockRequests.end()) {
        // 如果已经有排他锁，申请成功
        if (pos->lock_mode_ == LockMode::EXCLUSIVE) {
            return true;
        }
            // 只允许存在一个事务才能升级表写锁
        else if (lockRequests.size() == 1) {
            pos->lock_mode_ = LockMode::EXCLUSIVE;
            lockRequestQueue.group_lock_mode_ = GroupLockMode::X;
            return true;
        }
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
    }

    // 如果其他事务持有排他锁，则加锁失败(no-wait)
    if (lockRequestQueue.group_lock_mode_ != GroupLockMode::NON_LOCK) {
        // no-wait!
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        // lockRequestQueue.cv_.wait(lock);
    }
    // 设置队列锁模式为排他锁
    lockRequestQueue.group_lock_mode_ = GroupLockMode::X;
    // 添加当前事务的锁请求到队列中
    lockRequestQueue.request_queue_.emplace_back(txn->get_transaction_id(), LockMode::EXCLUSIVE);
    lockRequestQueue.request_queue_.back().granted_ = true;
    // 成功申请共享锁
    txn->get_lock_set()->emplace(lockDataId);
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

    GroupLockMode new_mode = GroupLockMode::NON_LOCK;
    for (auto& lockRequest : lockRequests) {
        new_mode = static_cast<GroupLockMode>(std::max(static_cast<int>(new_mode), static_cast<int>(lockRequest.lock_mode_)));
    }
    lockRequestQueue.group_lock_mode_ = new_mode;
    return true;
}