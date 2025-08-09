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

// 扫描算子
extern int scan_record_cnt;
class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    /*
        这里的beginTuple和nextTuple都是用来遍历所有的记录的,核心思想是找到满足条件的rid,并记录在数组中
    */

    size_t tupleLen() const override { return len_; };

    std::string get_tableName() override{ return tab_name_ ;}
    
    /* 这个函数的功能还没有太完善 */
    // 比较当前条件是否满足
    bool isTupleMatched(std::unique_ptr<RmRecord> &record, const Condition &cond) {
        TabCol lhsCol = cond.lhs_col;
        ColMeta lhsColMeta = *get_col(cols_, lhsCol);

        Value lhsVal = fetchColumnValue(record, lhsColMeta);    // 获取左列上的值

        if (cond.is_rhs_val) {
            Value rhsVal = cond.rhs_val;
            
            return cmpVal(lhsVal, rhsVal, cond.op);
        } else {
            TabCol rhsCol = cond.rhs_col;
            ColMeta rhsColMeta = *get_col(cols_, rhsCol);

            /* 右边可能是一个值列表,但是这里先不用管 */
            Value rhsVal = fetchColumnValue(record, rhsColMeta);
            return cmpVal(lhsVal, rhsVal, cond.op);
        }
        return false;
    }

    void beginTuple() override {
        for (scan_ = std::make_unique<RmScan>(fh_); !scan_->is_end(); scan_->next()) {
            // 检查当前记录是否满足条件 1.条件为空 2.满足fed_中的条件
            rid_ = scan_->rid();
            scan_record_cnt++;
            auto record = fh_->get_record(rid_, context_);
            if (fed_conds_.empty()) {
                return ;
            }
            for (auto& cond : fed_conds_) {
                // 对比当前记录和所有的条件
                if (!isTupleMatched(record, cond)) {
                    return ;
                } 
            }
        }
        return ;
    }

    void nextTuple() override {
        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            rid_ = scan_->rid();
            scan_record_cnt++;
            auto record = fh_->get_record(rid_, context_);
            if (fed_conds_.empty()) break;
            for (auto& cond : fed_conds_) {
                // 对比当前记录和所有的条件
                if (isTupleMatched(record, cond)) {
                    return ;
                } 
            }
        }
        return ;
    }
    bool is_end() const override { return scan_->is_end(); };

    std::unique_ptr<RmRecord> Next() override {
        
        return fh_->get_record(rid_, context_);;
    }

    Rid &rid() override { return rid_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }
};