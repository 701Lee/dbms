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

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool is_end() const { return false; };

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
};