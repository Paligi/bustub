//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    std::lock_guard<std::mutex> guard(latch_);

    size_t max_distance = 0;
    bool inf = false;
    bool evict = false;
    // auto candidate = node_store_.end();
    frame_id_t frame_to_evict{0}; 

    for (auto const &it : node_store_) {
        if (it.second.evictable()) {
            evict = true;
            if (it.second.history_entry() < k_){
                if (not (inf && max_distance >= (current_timestamp_ - it.second.history().back()))) {
                    inf = true;
                    max_distance = current_timestamp_ - it.second.history().back();
                    frame_to_evict = it.first;
                }
            } else if (!inf) { // LRU-K
                auto count = k_;
                size_t kth_stamp = 0;
                auto iterator = it.second.history();
                for (auto kth = iterator.rbegin(); count>0; kth++,count--) {
                if (count == 1) {
                    kth_stamp = *kth;
                }
            }
            if (max_distance < (current_timestamp_ - kth_stamp)) {
            max_distance = current_timestamp_ - kth_stamp;
            frame_to_evict = it.first;
            }
            }
        }
    }
    if (not evict) {
        return false;
    }
    *frame_id = frame_to_evict;
    node_store_.erase(frame_to_evict);
    curr_size_ --;
    return true;
}
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    std::lock_guard<std::mutex> guard(latch_);

    if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
        BUSTUB_ASSERT("id {} :out of replacer_size_ range", frame_id);
    }

    if (node_store_.find(frame_id) == node_store_.end()){
        node_store_.insert(std::make_pair(frame_id, LRUKNode()));       
    }

    node_store_.at(frame_id).Access(current_timestamp_);
    current_timestamp_ ++;


}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> guard(latch_);
    
    if (static_cast<size_t>(frame_id) >= this->replacer_size_ || frame_id < 0) {
        BUSTUB_ASSERT("id {} :out of replacer_size_ range", frame_id);
    }

    auto node = &node_store_[frame_id];

    if (node -> evictable() != set_evictable){
        node -> verse_evictable();
        curr_size_ += set_evictable ? 1 : -1;
    }

}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);

    //check if frame_id is in correct region
    if (static_cast<size_t>(frame_id) > replacer_size_ || frame_id <= 0) {
        BUSTUB_ASSERT("id {} :out of replacer_size_ range", frame_id);
    }

    if(node_store_.find(frame_id) == node_store_.end()){
        return;
    }
    // If not evictable, assert wrong message
    if(not node_store_.at(frame_id).evictable()){
        BUSTUB_ASSERT("id {} :not evictable when trying to remove it", frame_id);
    }
    // 
    node_store_.erase(frame_id);



}

auto LRUKReplacer::Size() -> size_t { 
    std::lock_guard<std::mutex> guard(latch_);
    return curr_size_;
}

}  // namespace bustub
