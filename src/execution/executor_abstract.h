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
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"
#include "defs.h"

class AbstractExecutor {
   public:
    Rid _abstract_rid;

    Context *context_;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; }

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool is_end() const{ return false; }

    virtual std::string get_tableName() {};//LMY：增加的虚函数，用来获取join表的表名

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    // 在一条记录中找到指定列的值
    Value fetchColumnValue(std::unique_ptr<RmRecord>& record, ColMeta &colMeta) {
        Value result;
        char *data = record->data + colMeta.offset;
        size_t len = colMeta.len;
        result.type = colMeta.type;
        switch(colMeta.type) {
            case TYPE_INT :
                int tmp_int;
                memcpy((char *)&tmp_int, data, len);
                result.set_int(tmp_int);
                break;
            case TYPE_FLOAT :
                float tmp_float;
                memcpy((char *)&tmp_float, data, len);
                result.set_float(tmp_float);
                break;
            case TYPE_STRING :
                std::string tmp(data, len);
                result.set_str(tmp);
                break;
        }
        result.init_raw(len);
        return result;
    }

    // 判断两个值是否满足给定的条件
    bool cmpVal(const Value &lhsVal, const Value &rhsVal, const CompOp &op) {
        if(!isComparable(lhsVal.type, rhsVal.type)) {
            throw IncompatibleTypeError(lhsVal.str_val, rhsVal.str_val);
        }
        switch (op) {
            case OP_EQ:
                return lhsVal == rhsVal;
            case OP_NE:
                return lhsVal != rhsVal;
            case OP_LT:
                return lhsVal < rhsVal;
            case OP_GT:
                return lhsVal > rhsVal;
            case OP_LE:
                return lhsVal <= rhsVal;
            case OP_GE:
                return lhsVal >= rhsVal;
            default:
                throw IncompatibleTypeError(coltype2str(lhsVal.type), coltype2str(rhsVal.type));
        }
        return false;
    }

    int compare(char *left_data, char *right_data, ColType left_type, ColType right_type, size_t len)
    {
        switch (left_type)
        {
        case ColType::TYPE_INT:
        {
            int left_int = *(int *)left_data;
            if (right_type == ColType::TYPE_INT)
            {
                int right_int = *(int *)right_data;
                return left_int > right_int ? 1 : (left_int < right_int ? -1 : 0);
            }
            else if (right_type == ColType::TYPE_FLOAT)
            {
                float right_float = *(float *)right_data;
                return left_int > right_float ? 1 : (left_int < right_float ? -1 : 0);
            }
        }
        break;
        case ColType::TYPE_FLOAT:
        {
            float left_float = *(float *)left_data;
            if (right_type == ColType::TYPE_FLOAT)
            {
                float right_float = *(float *)right_data;
                return left_float > right_float ? 1 : (left_float < right_float ? -1 : 0);
            }
            if (right_type == ColType::TYPE_INT)
            {
                int right_int = *(int *)right_data;
                return left_float > right_int ? 1 : (left_float < right_int ? -1 : 0);
            }
        }
        break;
        case ColType::TYPE_STRING:
        {
            if (right_type == ColType::TYPE_STRING)
            {
                return memcmp(left_data, right_data, len);
            }
        }
        break;
        default:
            break;
        }
    }
    /*判断满不满足单个表的where条件*/
    bool do_pridict(const std::unique_ptr<RmRecord> &record, std::vector<Condition> conds_,
                    std::vector<ColMeta> cols)
    {
        if (conds_.size() == 0)
        {
            return true;
        }
        char *record_data = record->data;
        ColType right_type;
        ColType left_type;
        for (auto cond : conds_)
        {
            auto clo_meta = get_col(cols, cond.lhs_col);

            char *left_data = (record_data + clo_meta->offset);
            left_type = clo_meta->type;

            char *right_data;
            if (cond.is_rhs_val)
            {
                right_data = cond.rhs_val.raw->data;
                right_type = cond.rhs_val.type;
            }
            else
            {
                auto right_colMeta = get_col(cols, cond.rhs_col);
                right_data = (record_data + right_colMeta->offset);
                right_type = right_colMeta->type;
            }
            int result = compare(left_data, right_data, left_type, right_type, clo_meta->len);
            switch (cond.op)
            {
            case CompOp::OP_EQ:
                result = (result == 0);
                break;
            case CompOp::OP_GE:
                result = (result >= 0);
                break;
            case CompOp::OP_GT:
                result = (result > 0);
                break;
            case CompOp::OP_LE:
                result = (result <= 0);
                break;
            case CompOp::OP_LT:
                result = (result < 0);
                break;
            case CompOp::OP_NE:
                result = (result != 0);
                break;
            }
            if (result == 0)
                return false;
        }
        return true;
    }

};