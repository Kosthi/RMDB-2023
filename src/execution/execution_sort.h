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

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_; // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    std::vector<ColMeta> order_cols_;
    size_t tuple_num;
    std::vector<bool> is_desc_;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;
    std::vector<std::unique_ptr<RmRecord>> tuples_;
    std::vector<ColType> col_types;
    std::vector<int> col_lens;
    size_t levels; // 排序等级
    std::vector<std::pair<int, int>> intervals; // 排序区间

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> sel_cols, std::vector<bool> is_desc) {
        prev_ = std::move(prev);
        cols_ = prev_->cols();
        order_cols_ = prev_->get_col_offset(sel_cols);
        is_desc_ = std::move(is_desc);
        tuple_num = 0;
        used_tuple.clear();
        levels = 0;
    }

    void beginTuple() override {

        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            tuples_.emplace_back(prev_->Next());
        }
        if (tuples_.empty()) return;

        tuple_num = 0;
        used_tuple.clear();

        for (auto& order_col : order_cols_) {
            col_types.emplace_back(order_col.type);
            col_lens.emplace_back(order_col.len);
        }

        intervals.emplace_back(std::pair{0, tuples_.size()});

        while (levels < order_cols_.size()) {
            // 第1 - n级排序
            auto curr_interval = intervals;
            intervals.clear();
            for (auto& [l, r] : curr_interval) {
                quicksort(tuples_, l, r - 1);
                if (levels + 1 < order_cols_.size()) {
                    for (int i = l, j = l + 1; j < r; ++j) {
                        while (j < r && !compare(tuples_[i]->data, tuples_[j]->data, true)) {
                            ++j;
                        }
                        if (j - i > 1) {
                            intervals.emplace_back(i, j);
                        }
                        i = j;
                    }
                }
            }
            if (intervals.empty()) return;
            ++levels; // 排序等级提升
        }
    }

    void nextTuple() override {
        ++tuple_num;
    }

    std::unique_ptr<RmRecord> Next() override {
        assert(!tuples_.empty());
        return std::move(tuples_[tuple_num]);
    }

    Rid &rid() override { return _abstract_rid; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return tuples_.empty() || tuple_num == tuples_.size(); }

    // Quicksort algorithm for sorting the data
    void quicksort(std::vector<std::unique_ptr<RmRecord>>& tuples, int l, int r) {
        if (l >= r) return;
        char* x = tuples[l + (r - l) / 2]->data;
        int i = l - 1, j = r + 1;
        while (i < j) {
            do ++i; while (compare(tuples[i]->data, x, true));
            do --j; while (compare(tuples[j]->data, x, false));
            if (i < j) std::iter_swap(&tuples[i], &tuples[j]);
        }
        quicksort(tuples, l, j);
        quicksort(tuples, j + 1, r);
    }

    inline bool compare(const char* a, const char* b, bool is_LT) {
        int res = compare(a + order_cols_[levels].offset, b + order_cols_[levels].offset, col_lens[levels],
                          col_types[levels]);
        if (res != 0) {
            return is_LT ? (is_desc_[levels] ? res > 0 : res < 0) : (is_desc_[levels] ? res < 0 : res > 0);
        }
        return false;
    }

    /**
    * @description: 比较数据数值
    *
    * @return std::unique_ptr<RmRecord>
    */
    static inline int compare(const char* a, const char* b, int col_len, ColType col_type) {
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
};