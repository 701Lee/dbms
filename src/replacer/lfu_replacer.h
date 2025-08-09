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

#include <list>
#include <mutex>
#include <iterator>
#include <unordered_map>
#include <map>

#include "common/config.h"
#include "replacer/replacer.h"

class LFUReplacer : public Replacer {
public:
    /**
     * @description: 创建一个新的 lfuReplacer
     * @param {size_t} num_pages lfuReplacer 最多需要存储的page数量
     */
    explicit LFUReplacer(size_t num_pages);

    ~LFUReplacer();
    
    bool victim(frame_id_t *frame_id);

    void pin(frame_id_t frame_id);

    void unpin(frame_id_t frame_id);

    size_t Size();
private:
    std::mutex latch_;              // 互斥锁
    size_t max_size_;               // 最大容量,和缓冲池大小相同
    size_t min_freq;                // 页面出现次数的最小值，用于淘汰
    std::unordered_map<frame_id_t, std::pair<size_t, std::list<frame_id_t>::iterator>> pageMap;         // key -> {freq, iterator}
    // 我们在 unpin 时 push_front（新在前），所以淘汰时从 back（旧的）出队，实现 LRU
    std::map<size_t, std::list<frame_id_t>> freqMap;              // freq -> list{page}
};