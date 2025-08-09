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

class NestedLoopJoinExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> left_;  // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_; // 右儿子节点（需要join的表）
    size_t len_;                              // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;               // join后获得的记录的字段

    std::vector<Condition> fed_conds_; // join条件
    bool isend;

    std::vector<std::unique_ptr<RmRecord>> left_record;
    std::vector<std::unique_ptr<RmRecord>> right_record;
    std::vector<std::unique_ptr<RmRecord>> all_record; // 存储所有的拼接好的record
    int position;

public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds)
    {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols)
        {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

        position = 0;
    }

    void beginTuple() override
    {
        // 1.填充 left_record 和 right_record
        for (left_->beginTuple(); !left_->is_end(); left_->nextTuple())
        {
            left_record.push_back(std::move(left_->Next()));
        }
        for (right_->beginTuple(); !right_->is_end(); right_->nextTuple())
        {
            right_record.push_back(std::move(right_->Next()));
        }

        // 2.填充all_record
        for (auto i = 0; i < left_record.size(); ++i)
        {
            for (auto j = 0; j < right_record.size(); ++j)
            {
                auto left_Tuple = left_record[i].get();
                auto right_Tuple = right_record[j].get();
                auto record = std::make_unique<RmRecord>(len_);
                memcpy(record->data, left_Tuple->data, left_Tuple->size);
                memcpy(record->data + left_Tuple->size, right_Tuple->data, right_Tuple->size);

                if (do_pridict(record, fed_conds_, cols_))
                {
                    all_record.push_back(std::move(record));
                }
            }
        }
        // 时间复杂度 o(m+n+mn) 看起来还不错
        // 空间复杂度也是这么多
    }

    void nextTuple() override
    {
        position++;
    }

    std::unique_ptr<RmRecord> Next() override
    {
        return std::move(all_record[position]);
    }

    Rid &rid() override { return _abstract_rid; }
    std::string getType() override { return "Nestedloop_Scan"; }
    bool is_end() const override { return (position == all_record.size()); }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override{
        return cols_;
    }
};