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

#define JOIN_BUFFER_SIZE 8388608

class BlockNestedLoopJoinExecutor : public AbstractExecutor {
private:
    // left_, right_ seq_scan或index_scan 的实例
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件

    std::vector<std::unique_ptr<RmRecord>> left_block_;
    std::vector<std::unique_ptr<RmRecord>> right_block_;
    size_t left_block_idx_;
    size_t right_block_idx_;

public:
    BlockNestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        fed_conds_ = std::move(conds);

        left_block_idx_ = 0;
        right_block_idx_ = 0;
    }

    void beginTuple() override {
        left_->beginTuple();
        if (left_->is_end()) {
            return;
        }
        right_->beginTuple();
        if (right_->is_end()) {
            return;
        }

        while (!left_->is_end() && left_block_.size() < JOIN_BUFFER_SIZE) {
            left_block_.emplace_back(left_->Next());
            left_->nextTuple();
        }

        while (!right_->is_end() && right_block_.size() < JOIN_BUFFER_SIZE) {
            right_block_.emplace_back(right_->Next());
            right_->nextTuple();
        }

        if (fed_conds_.empty()) return;

        while (!is_end()) {
            if (!cmp_conds(left_block_[left_block_idx_].get(), right_block_[right_block_idx_].get(), fed_conds_, cols_)) {
                ++right_block_idx_;
                if (right_block_idx_ >= right_block_.size()) {
                    // 读新的right
                    right_block_idx_ = 0;
                    right_block_.clear();
                    while (!right_->is_end() && right_block_.size() < JOIN_BUFFER_SIZE) {
                        right_block_.emplace_back(right_->Next());
                        right_->nextTuple();
                    }
                    // 如果right读完了
                    if (right_block_.empty()) {
                        // left + 1, right从头开始读
                        ++left_block_idx_;
                        // 如果left块读完了, 读新的left
                        if (left_block_idx_ >= left_block_.size()) {
                            left_block_idx_ = 0;
                            left_block_.clear();
                            while (!left_->is_end() && left_block_.size() < JOIN_BUFFER_SIZE) {
                                left_block_.emplace_back(left_->Next());
                                left_->nextTuple();
                            }
                            // 如果left读完了，直接退出
                            if (left_block_.empty()) {
                                return;
                            }
                        }
                        right_->beginTuple();
                        while (!right_->is_end() && right_block_.size() < JOIN_BUFFER_SIZE) {
                            right_block_.emplace_back(right_->Next());
                            right_->nextTuple();
                        }
                    }
                }
                continue;
            }
            return;
        }
    }

    void nextTuple() override {
        assert(!is_end());
        ++right_block_idx_;

        if (right_block_idx_ >= right_block_.size()) {
            // 读新的right
            right_block_idx_ = 0;
            right_block_.clear();
            while (!right_->is_end() && right_block_.size() < JOIN_BUFFER_SIZE) {
                right_block_.emplace_back(right_->Next());
                right_->nextTuple();
            }
            // 如果right读完了
            if (right_block_.empty()) {
                // left + 1, right从头开始读
                ++left_block_idx_;
                // 如果left块读完了, 读新的left
                if (left_block_idx_ >= left_block_.size()) {
                    left_block_idx_ = 0;
                    left_block_.clear();
                    while (!left_->is_end() && left_block_.size() < JOIN_BUFFER_SIZE) {
                        left_block_.emplace_back(left_->Next());
                        left_->nextTuple();
                    }
                    // 如果left读完了，直接退出
                    if (left_block_.empty()) {
                        return;
                    }
                }
                right_->beginTuple();
                while (!right_->is_end() && right_block_.size() < JOIN_BUFFER_SIZE) {
                    right_block_.emplace_back(right_->Next());
                    right_->nextTuple();
                }
            }
        }

        if (fed_conds_.empty()) return;

        while (!is_end()) {
            if (!cmp_conds(left_block_[left_block_idx_].get(), right_block_[right_block_idx_].get(), fed_conds_, cols_)) {
                ++right_block_idx_;
                if (right_block_idx_ >= right_block_.size()) {
                    // 读新的right
                    right_block_idx_ = 0;
                    right_block_.clear();
                    while (!right_->is_end() && right_block_.size() < JOIN_BUFFER_SIZE) {
                        right_block_.emplace_back(right_->Next());
                        right_->nextTuple();
                    }
                    // 如果right读完了
                    if (right_block_.empty()) {
                        // left + 1, right从头开始读
                        ++left_block_idx_;
                        // 如果left块读完了, 读新的left
                        if (left_block_idx_ >= left_block_.size()) {
                            left_block_idx_ = 0;
                            left_block_.clear();
                            while (!left_->is_end() && left_block_.size() < JOIN_BUFFER_SIZE) {
                                left_block_.emplace_back(left_->Next());
                                left_->nextTuple();
                            }
                            // 如果left读完了，直接退出
                            if (left_block_.empty()) {
                                return;
                            }
                        }
                        right_->beginTuple();
                        while (!right_->is_end() && right_block_.size() < JOIN_BUFFER_SIZE) {
                            right_block_.emplace_back(right_->Next());
                            right_->nextTuple();
                        }
                    }
                }
                continue;
            }
            return;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        assert(!is_end());
        auto left_rec = left_block_[left_block_idx_].get();
        auto right_rec = std::move(right_block_[right_block_idx_]);
        auto join_rec = std::make_unique<RmRecord>(len_);
        memcpy(join_rec->data, left_rec->data, left_->tupleLen());
        memcpy(join_rec->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
        return join_rec;
    }

    // 判断是否满足单个谓词条件
    bool cmp_cond(const RmRecord* lrec, const RmRecord* rrec, const Condition& cond,  const std::vector<ColMeta>& rec_cols) {
        // 提取左值与右值的数据和类型
        auto lhs_col_meta = get_col(rec_cols, cond.lhs_col);
        auto rhs_col_meta = get_col(rec_cols, cond.rhs_col);
        // 要以参数形式传递得到记录，不能直接left_->Next()，否则 data 地址是不正确的
        auto lhs_data = lrec->data + lhs_col_meta->offset;
        auto rhs_data = rrec->data + rhs_col_meta->offset - left_->tupleLen();
        ColType rhs_type = rhs_col_meta->type;
        // 判断左右值数据类型是否相同
        if (lhs_col_meta->type != rhs_type) {
            return false;
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
    bool cmp_conds(const RmRecord* lrec, const RmRecord* rrec, const std::vector<Condition>& conds,  const std::vector<ColMeta>& rec_cols) {
        return std::all_of(conds.begin(), conds.end(), [&](const Condition &cond) {
            return cmp_cond(lrec, rrec, cond, rec_cols);
        });
    }

    /**
    * @description: 比较数据数值
    *
    * @return std::unique_ptr<RmRecord>
    */
    static int compare(const char* a, const char* b, int col_len, ColType col_type) {
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

    Rid &rid() override { return _abstract_rid; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return left_block_.empty() || right_block_.empty(); }
};