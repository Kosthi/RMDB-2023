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

class AggregationExecutor : public AbstractExecutor {
private:
    // scan或sort或join
    std::unique_ptr<AbstractExecutor> prev_;    //
    std::vector<ColMeta> cols_;                 // 所有列
    std::vector<ColMeta> sel_cols_;             // 聚合的列
    std::vector<AggType> types_;                // 聚合类型
    bool end_;
    size_t len_;
    std::vector<RmRecord> recs_;
    int cnt_max_; // count(*) 缓存

public:
    AggregationExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> sel_cols, std::vector<AggType> types) {
        prev_ = std::move(prev);
        cols_ = prev_->cols();
        sel_cols_ = get_cols_meta(sel_cols);
        types_ = std::move(types);
        end_ = false;
        // len_ = prev_->tupleLen();
        for (size_t i = 0; i < sel_cols_.size(); ++i) {
            if (types_[i] == T_COUNT) {
                len_ += sizeof(int);
            } else {
                len_ += sel_cols_[i].len;
            }
        }
        cnt_max_ = -1;
    }

    void beginTuple() override {
        // get all rec.
        prev_->beginTuple();
        while (!prev_->is_end()) {
            auto tuple = prev_->Next();
            recs_.emplace_back(*tuple);
            prev_->nextTuple();
        }
    }

    void nextTuple() override {
        // 仅执行一次
        end_ = true;
    }

    std::unique_ptr<RmRecord> Next() override {
        // 初始化计算len_
        RmRecord rec(len_);
        size_t offset = 0;
        for (size_t i = 0; i < sel_cols_.size(); ++i) {
            switch (types_[i]) {
                case T_SUM: {
                    auto &col_len = sel_cols_[i].len;
                    auto &col_type = sel_cols_[i].type;
                    char* sum = new char[col_len];
                    char* tmp = new char[col_len];
                    memset(sum, 0, col_len);
                    // 只涉及int, float.
                    for (auto &rec_ : recs_) {
                        memcpy(tmp, rec_.data + sel_cols_[i].offset, col_len);
                        if (col_type == TYPE_INT) {
                            *(int*)sum += *(int*)tmp;
                        } else if (col_type == TYPE_FLOAT) {
                            *(double*)sum += *(double*)(tmp);
                        }
                    }
                    memcpy(rec.data + offset, sum, col_len);
                    sel_cols_[i].offset = offset;
                    offset += col_len;
                    delete[] sum;
                    delete[] tmp;
                    break;
                }
                case T_MAX: {
                    auto &col_len = sel_cols_[i].len;
                    auto &col_type = sel_cols_[i].type;
                    char* value = new char[col_len];
                    memset(value, 0, col_len);
                    char* tmp = new char[col_len];
                    // 只涉及int, float, char
                    for (auto &rec_ : recs_) {
                        memcpy(tmp, rec_.data + sel_cols_[i].offset, col_len);
                        if (col_type == TYPE_INT && *(int*)value < *(int*)tmp) {
                            *(int*)value = *(int*)tmp;
                        } else if (col_type == TYPE_FLOAT && *(double*)value < *(double*)tmp) {
                            *(double*)value = *(double*)(tmp);
                        } else if (col_type == TYPE_STRING && memcmp(value, tmp, col_len) < 0) {
                            memcpy(value, tmp, col_len);
                        }
                    }
                    memcpy(rec.data + offset, value, col_len);
                    sel_cols_[i].offset = offset;
                    offset += col_len;
                    delete[] value;
                    delete[] tmp;
                    break;
                }
                case T_MIN: {
                    auto &col_len = sel_cols_[i].len;
                    auto &col_type = sel_cols_[i].type;
                    char* value = new char[col_len];
                    // 初始化为最大值
                    memset(value, 127, col_len);
                    char* tmp = new char[col_len];
                    // 只涉及int, float, char
                    for (auto &rec_ : recs_) {
                        memcpy(tmp, rec_.data + sel_cols_[i].offset, col_len);
                        if (col_type == TYPE_INT && *(int*)value > *(int*)tmp) {
                            *(int*)value = *(int*)tmp;
                        } else if (col_type == TYPE_FLOAT && *(double*)value > *(double*)tmp) {
                            *(double*)value = *(double*)(tmp);
                        } else if (col_type == TYPE_STRING && memcmp(value, tmp, col_len) > 0) {
                            memcpy(value, tmp, col_len);
                        }
                    }
                    memcpy(rec.data + offset, value, col_len);
                    sel_cols_[i].offset = offset;
                    offset += col_len;
                    delete[] value;
                    delete[] tmp;
                    break;
                }
                case T_COUNT: {
                    // count 列，都用 int 类型存
                    // 2. count(*)
                    if (sel_cols_[i].tab_name.empty() && sel_cols_[i].name.empty()) {
                        if (cnt_max_ != -1) break;
                        // count所有列，找到最大值
                        for (auto &col_ : cols_) {
                            int cnt = 0;
                            auto &col_len = col_.len;
                            auto &col_type = col_.type;
                            char *tmp = new char[col_len];
                            // 只涉及int, float, char
                            for (auto &rec_: recs_) {
                                memcpy(tmp, rec_.data + col_.offset, col_len);
                                // int, float 不存在空的值，只需要特判string == ""
                                if (col_type != TYPE_STRING) {
                                    cnt++;
                                } else {
                                    std::string str(tmp, col_len);
                                    if (!str.empty()) {
                                        cnt++;
                                    }
                                }
                            }
                            if (cnt_max_ < cnt) {
                                cnt_max_ = cnt;
                            }
                            delete[] tmp;
                        }
                        memcpy(rec.data + offset, &cnt_max_, sizeof(int));
                        sel_cols_[i].offset = offset;
                        offset += sizeof(int);
                        sel_cols_[i].type = TYPE_INT;
                        break;
                    }
                    // 1. count(col)
                    auto &col_len = sel_cols_[i].len;
                    auto &col_type = sel_cols_[i].type;
                    int cnt = 0;
                    char* tmp = new char[col_len];
                    // 只涉及int, float, char
                    for (auto &rec_ : recs_) {
                        memcpy(tmp, rec_.data + sel_cols_[i].offset, col_len);
                        // int, float 不存在空的值，只需要特判string == ""
                        if (col_type != TYPE_STRING) {
                            cnt++;
                        } else {
                            std::string str(tmp);
                            if (!str.empty()) {
                                cnt++;
                            }
                        }
                    }
                    memcpy(rec.data + offset, &cnt, sizeof(int));
                    sel_cols_[i].offset = offset;
                    offset += sizeof(int);
                    sel_cols_[i].type = TYPE_INT;
                    delete[] tmp;
                    break;
                }
            }
        }
        return std::make_unique<RmRecord>(rec);
    }

    Rid &rid() override { return _abstract_rid; }

    const std::vector<ColMeta> &cols() const override { return sel_cols_; }

    bool is_end() const override { return end_; }

    std::vector<ColMeta> get_cols_meta(std::vector<TabCol>& targets)  {
        std::vector<ColMeta> cols_meta;
        for (auto& target : targets) {
            if (target.tab_name.empty() && target.col_name.empty()) {
                cols_meta.emplace_back(ColMeta());
                continue;
            }
            cols_meta.emplace_back(*get_col(cols_, target));
        }
        return cols_meta;
    }
};
