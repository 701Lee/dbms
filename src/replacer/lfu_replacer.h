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

#include <mutex>
#include <vector>
#include <optional>
#include <unordered_map>

#include "common/config.h"
#include "replacer/replacer.h"


class lfuReplacer : public Replacer {
public:
    /**
     * @description: 创建一个新的 lfuReplacer
     * @param {size_t} num_pages lfuReplacer 最多需要存储的page数量
     */
    explicit lfuReplacer(size_t num_pages);

    ~lfuReplacer();
    
    bool victim(frame_id_t *frame_id);

    void pin(frame_id_t frame_id);

    void unpin(frame_id_t frame_id);

    size_t Size();

private:
    std::mutex latch_;              // 互斥锁
};