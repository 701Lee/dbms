#include "lfu_replacer.h"

LFUReplacer::LFUReplacer(size_t num_pages) : max_size_(num_pages), min_freq(0){}

LFUReplacer::~LFUReplacer() = default;

/**
 * @description: 使用LFU策略删除一个victim frame，并返回该frame的id
 * @param {frame_id_t*} frame_id 被移除的frame的id，如果没有frame被移除返回nullptr
 * @return {bool} 如果成功淘汰了一个页面则返回true，否则返回false
 */
bool LFUReplacer::victim(frame_id_t *frame_id){
    // C++17 std::scoped_lock
    // 它能够避免死锁发生，其构造函数能够自动进行上锁操作，析构函数会对互斥量进行解锁操作，保证线程安全。
    std::scoped_lock lock{latch_};  //  如果编译报错可以替换成其他lock

    // Todo:
    //  利用lfu_replacer中的pageHash_,freqHash_实现LFU策略
    //  选择合适的frame指定为淘汰页面,赋值给*frame_id
    if (Size() == 0) {
        return false;
    }

    frame_id_t old_frame_id = freqMap[min_freq].back();
    freqMap[min_freq].pop_back();
    pageMap.erase(old_frame_id);

    if (freqMap[min_freq].empty()) {
        freqMap.erase(min_freq);
        min_freq = freqMap.begin()->first;
    }
    *frame_id = old_frame_id;
    return true;
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} 需要固定的frame的id
 */
void LFUReplacer::pin(frame_id_t frame_id){
    std::scoped_lock lock{latch_};
    if (!pageMap.count(frame_id)) {
        return ;
    }
    auto& [freq, iter] = pageMap[frame_id];
    freqMap[freq].erase(iter);
    pageMap.erase(frame_id);

    if (freqMap[min_freq].empty()) {
        freqMap.erase(min_freq);
        min_freq = freqMap.begin()->first;
    }
}

/**
 * @description: 取消固定一个frame，代表该页面可以被淘汰
 * @param {frame_id_t} frame_id 取消固定的frame的id
 */
void LFUReplacer::unpin(frame_id_t frame_id){
    std::scoped_lock lock{latch_};
    if (pageMap.find(frame_id) != pageMap.end()) {
        auto& [freq, iter] = pageMap[frame_id];
        freqMap[freq].erase(iter);
        freq++;
        freqMap[freq].push_front(frame_id);
        pageMap[frame_id] = {freq, freqMap[freq].begin()};
        
        if (freqMap[min_freq].empty()) {
            min_freq++;
        }
        return ;
    }
    if (Size() >= max_size_) {
        return ;
    }
    freqMap[1].push_front(frame_id);
    pageMap[frame_id] = {1, freqMap[1].begin()};
    min_freq = 1;
    return ;
}

/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 */
size_t LFUReplacer::Size(){
    return pageMap.size();
}
