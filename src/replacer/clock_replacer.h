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

struct ClockNode {
    frame_id_t frame_id;
    bool use_bit;
};

class ClockReplacer : public Replacer {
public:
    /**
     * @description: 创建一个新的 ClockReplacer
     * @param {size_t} num_pages ClockReplacer 最多需要存储的page数量
     */
    explicit ClockReplacer(size_t num_pages);

    ~ClockReplacer();
    
    bool victim(frame_id_t *frame_id);

    void pin(frame_id_t frame_id);

    void unpin(frame_id_t frame_id);

    size_t Size();

private:
    std::vector<std::optional<ClockNode>> clock_buffer_;      // 记录缓冲池中页的信息
    size_t clock_hand_;             // clock_buffer_ 数组指针
    size_t max_size_;               // 最大容量（与缓冲池的容量相同）
    size_t curr_size_;              // 当前未被 pin 的页面数量(可以淘汰的页面数量)
    std::unordered_map<frame_id_t, size_t> frame_to_index_;   // frame_id_t -> clock_buffer_ index
    std::mutex latch_;              // 互斥锁
};