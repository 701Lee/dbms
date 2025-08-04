#include "clock_replacer.h"

ClockReplacer::ClockReplacer(size_t num_pages) : clock_buffer_(num_pages), clock_hand_(0), max_size_(num_pages), curr_size_(0) {
    // 初始化 vector 为指定大小，每个位置初始为 std::nullopt
    for (size_t i = 0; i < num_pages; ++i) {
        clock_buffer_[i] = std::nullopt;
    }
}

ClockReplacer::~ClockReplacer() = default;

/**
 * @description: 使用CLOCK策略删除一个victim frame，并返回该frame的id
 * @param {frame_id_t*} frame_id 被移除的frame的id，如果没有frame被移除返回nullptr
 * @return {bool} 如果成功淘汰了一个页面则返回true，否则返回false
 */
bool ClockReplacer::victim(frame_id_t *frame_id) {
    std::scoped_lock lock{latch_};

    if (Size() == 0) {
        return false;
    }

    // 持续扫描，直到找到 use_bit == 0 的页面
    while (true) {
        auto& node_opt = clock_buffer_[clock_hand_];
        if (node_opt.has_value()) {
            ClockNode& node = node_opt.value();
            if (node.use_bit) {
                node.use_bit = false; 
            } else {
                *frame_id = node.frame_id;
                frame_to_index_.erase(node.frame_id);
                node_opt.reset();
                curr_size_--;
                clock_hand_ = (clock_hand_ + 1) % max_size_;
                return true;
            }
        }
        clock_hand_ = (clock_hand_ + 1) % max_size_;
    }
    return false;
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} 需要固定的frame的id
 */
void ClockReplacer::pin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};
    auto it = frame_to_index_.find(frame_id);
    if (it == frame_to_index_.end()) {
        return ;
    }

    size_t idx = it->second;
    if (clock_buffer_[idx].has_value()) {
        clock_buffer_[idx].reset();
        frame_to_index_.erase(frame_id);
        if (Size() > 0) {
            curr_size_--;
        }
    }
}

/**
 * @description: 取消固定一个frame，代表该页面可以被淘汰
 * @param {frame_id_t} frame_id 取消固定的frame的id
 */
void ClockReplacer::unpin(frame_id_t frame_id) {
    std::scoped_lock lock{latch_};
    if (frame_to_index_.count(frame_id)) return ;
    if (Size() >= max_size_) {
        return ;
    }

    for (size_t i = 0; i < max_size_; i++) {
        size_t idx = (clock_hand_ + i) % max_size_;
        if (!clock_buffer_[idx].has_value()) {
            clock_buffer_[idx].emplace(ClockNode{frame_id, true});
            frame_to_index_[frame_id] = idx;
            curr_size_++;
            return ; 
        }
    }
    return ;
}

/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 */
size_t ClockReplacer::Size() {
    return curr_size_;
}
