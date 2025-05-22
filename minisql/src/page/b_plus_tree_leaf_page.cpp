#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetKeySize(key_size);
    SetSize(0); // Initialized as empty.
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
    SetKeySize(key_size);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
    int cur_size = GetSize();
    if (cur_size == 0) { return 0; }

    // Binary search.
    int left = 0;
    int right = cur_size - 1;
    while (left <= right) {
        int mid = (left + right) / 2;
        GenericKey *mid_key = KeyAt(mid);

        if (KM.CompareKeys(key, mid_key) < 0) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    return right;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
    int cur_size = GetSize();

    // Find the position to insert.
    int index = KeyIndex(key, KM);

    // Check if key already exists.
    if (index < cur_size && KM.CompareKeys(key, KeyAt(index)) == 0) {
        SetValueAt(index, value);
        return cur_size;
    }

    // Make space for the new key-value pair.
    for (int i = cur_size; i > index; i--) {
        SetKeyAt(i, KeyAt(i - 1));
        SetValueAt(i, ValueAt(i - 1));
    }

    // Insert the new key-value pair.
    SetKeyAt(index, key);
    SetValueAt(index, value);

    IncreaseSize(1);

    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
    int cur_size = GetSize();
    int move_size = cur_size / 2;
    int start_index = cur_size - move_size;
    recipient->CopyNFrom(PairPtrAt(start_index), move_size);
    SetSize(cur_size - move_size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
    // Caulculate the starting position.
    int start_index = GetSize();

    // Copy the pairs.
    PairCopy(PairPtrAt(start_index), src, size);

    IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
    int index = KeyIndex(key, KM);

    // if (index < GetSize() && KM.CompareKeys(key, KeyAt(index)) == 0) { // Found.
    if (index < GetSize() && index >= 0) {
        value = ValueAt(index);
        return true;
    }
    
    // Not Found.
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
    int cur_size = GetSize();
    int index = KeyIndex(key, KM);

    // Check if the index exists and matches.
    if (index >= cur_size || KM.CompareKeys(key, KeyAt(index)) == 0) { return GetSize(); }

    // Move forward.
    for (int i = index + 1; i < cur_size; i++) {
        SetKeyAt(i - 1, KeyAt(i));
        SetValueAt(i - 1, ValueAt(i));
    }

    SetSize(cur_size - 1);
    
    return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
    // Copy all pairs to the recipients.
    recipient->CopyNFrom(pairs_off, GetSize());
    
    // Update the next_page id in the sibling page.
    recipient->SetNextPageId(GetNextPageId());

    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
    int cur_size = GetSize();

    // Copy the first key-value pair to the recipient page.
    recipient->CopyLastFrom(KeyAt(0), ValueAt(0));

    // Move forward.
    for (int i = 1; i < cur_size; i++) {
        SetKeyAt(i - 1, KeyAt(i));
        SetValueAt(i - 1, ValueAt(i));
    }

    SetSize(cur_size - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
    int index = GetSize();

    SetKeyAt(index, key);
    SetValueAt(index, value);

    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
    int last_index = GetSize() - 1;
    recipient->CopyFirstFrom(KeyAt(last_index), ValueAt(last_index));
    SetSize(last_index);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
    // Make space for the new entry.
    for (int i = GetSize(); i > 0; i--) {
        SetKeyAt(i, KeyAt(i - 1));
        SetValueAt(i, ValueAt(i - 1));
    }

    // Insert the new key-value pair.
    SetKeyAt(0, key);
    SetValueAt(0, value);

    IncreaseSize(1);
}