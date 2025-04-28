#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  /**
   * Remove the least recently used page, return the page id in Page in Buffer Pool.
   */
  bool Victim(frame_id_t *frame_id) override;

  /**
   * Can't be removed when pinned <==> removed from lru_list_.
   */
  void Pin(frame_id_t frame_id) override;

  /**
   * Unpinned.
   */
  void Unpin(frame_id_t frame_id) override;

  /**
   * Return the number of pages that can be removed.
   */
  size_t Size() override;

private:
  // add your own private member variables here
  /**
   * The most recently used page is linked at the end, while the least recently used page is linked at the head.
   */
  std::list<frame_id_t> lru_list_; 
  /**
   * Hash table to rapidly judge wheter a frame_id is in lru_list_.
   */
  std::unordered_set<frame_id_t> lru_set_;
};

#endif  // MINISQL_LRU_REPLACER_H
