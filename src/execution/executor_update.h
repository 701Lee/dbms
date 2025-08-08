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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    /**
     *  rids_已经是根据where条件筛选后的记录元数据
     */
    std::unique_ptr<RmRecord> Next() override {
        /** update操作的逻辑
         *  首先rids_是已经根据where条件筛选出来的记录元数据,我们的目的是将筛选出来的每个记录中对应列的数据修改为set的新值
         */
        for (size_t i = 0; i < rids_.size(); i++) {
            std::unique_ptr<RmRecord> record = fh_->get_record(rids_[i], context_);

            for (auto clause : set_clauses_) {
                auto rhs_val = clause.rhs;
                auto lhs_col = tab_.get_col(clause.lhs.col_name);
                
                // 考虑 int <=> float相互转换
                if (rhs_val.type != lhs_col->type) {
                    if (rhs_val.type == TYPE_INT) {
                        rhs_val.float_val = rhs_val.int_val;
                    } else if (rhs_val.type == TYPE_FLOAT) {
                        rhs_val.int_val = rhs_val.float_val;
                    }
                }
                // 这里默认lhs_col->type 和 rhs_val.type 相同
                if (lhs_col->type == TYPE_STRING) {
                    memcpy(record->data + lhs_col->offset, rhs_val.str_val.c_str(), lhs_col->len);
                } else if (lhs_col->type == TYPE_INT) {
                    memcpy(record->data + lhs_col->offset, &rhs_val.int_val, lhs_col->len);
                } else {
                    memcpy(record->data + lhs_col->offset, &rhs_val.float_val, lhs_col->len);
                }
            }
            fh_->update_record(rids_[i], record->data, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

    virtual std::string getType() override { return "UpdataExecutor"; };
};