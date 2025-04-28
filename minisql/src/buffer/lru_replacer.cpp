#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * Finished
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
    if (lru_list_.empty())
        return false;
    
    *frame_id = lru_list_.front();
    lru_list_.pop_front();
    lru_set_.erase(*frame_id);
    return true;
}

/**
 * Finished
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
    if (lru_set_.find(frame_id) != lru_set_.end()) {
        lru_list_.remove(frame_id);
        lru_set_.erase(frame_id);
    }
}

/**
 * Finished
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
    if (lru_set_.find(frame_id) == lru_set_.end()) {
        lru_list_.push_back(frame_id);
        lru_set_.insert(frame_id);
    }
}

/**
 * Finished
 */
size_t LRUReplacer::Size() {
    return lru_set_.size();
}