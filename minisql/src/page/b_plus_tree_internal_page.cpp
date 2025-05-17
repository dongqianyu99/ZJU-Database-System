#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetKeySize(key_size);
    SetSize(0); // Initialized as empty.
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
    return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
    int cur_size = GetSize();
    if (cur_size == 0) { return INVALID_PAGE_ID; }
    if (cur_size ==1 ) { return ValueAt(0); }

    // Start the search from the second key.
    int left = 1;
    int right = GetSize() - 1;
    while (left <= right ) { // Binary search.
        int mid = (left + right) / 2;
        GenericKey *mid_key = KeyAt(mid);

        if (KM.CompareKeys(key, mid_key) < 0) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    return ValueAt(right);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    SetValueAt(0, old_value);
    SetKeyAt(1, new_key);
    SetValueAt(1, new_value);
    SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    int cur_size = GetSize();

    // Find the index of the old value.
    int index = ValueIndex(old_value);
    if (index = -1) // If not found.
        return cur_size;

    // Make space for the new key-value pair.
    for (int i = cur_size; i > index + 1; i--) {
        // Move backward.
        SetKeyAt(i, KeyAt(i - 1));
        SetValueAt(i, ValueAt(i - 1));
    }

    // Insert the new-key pair.
    SetKeyAt(index, new_key);
    SetValueAt(index, new_value);

    // Update the size.
    SetSize(cur_size + 1);

    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
    int cur_size = GetSize();
    int move_size = cur_size / 2;
    int start_index = cur_size - move_size;
    recipient->CopyNFrom(pairs_off + start_index * pair_size, move_size, buffer_pool_manager);
    SetSize(cur_size - move_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
    int start_index = GetSize();
    PairCopy(pairs_off + start_index * pair_size, src, size);
    // Update the parent id for all the children.
    for (int i = start_index; i < start_index + size; i++) {
        page_id_t child_page_id  = ValueAt(i);
        auto child_page = buffer_pool_manager->FetchPage(child_page_id);

        if (child_page != nullptr) {
          auto child_bPlusTree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
          child_bPlusTree_page->SetParentPageId(GetPageId());
          buffer_pool_manager->UnpinPage(child_page_id, true);
        }
    }
    SetSize(start_index + size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
    int cur_size = GetSize();
    // Check if index is valid.
    if (index < 0 || index >= cur_size) { return; }

    // Move forward.
    for (int i = index + 1; i < cur_size; i++) {
        SetKeyAt(i - 1, KeyAt(i));
        SetValueAt(i - 1, ValueAt(i));
    }

    // Update the size.
    SetSize(cur_size - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
    ASSERT(GetSize() == 1, "Make sure the page have exactly one child only!");
    page_id_t child = ValueAt(0);
    SetSize(0);
    return child;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
    int recipient_index = recipient->GetSize();
    // Insert the middle key to the first position.
    recipient->SetKeyAt(recipient_index, middle_key);
    // Copy all the remaining pairs.
    recipient->CopyNFrom(KeyAt(1), GetSize() - 1, buffer_pool_manager);

    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
    recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
    Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int index = GetSize();
    // Add a new key-value pair.
    SetKeyAt(index, key);
    SetValueAt(index, value);

    // Update the parent page id for the child.
    auto child_page = buffer_pool_manager->FetchPage(value);
    if (child_page != nullptr) {
        auto child_bPlusTree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_bPlusTree_page->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(value, true);
    }

    SetSize(index + 1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
    // Get the last key-value pair.
    int last_index = GetSize() - 1;
    page_id_t last_value = ValueAt(last_index);

    // Remove the last pair.
    SetSize(last_index);

    // Add to the front of recipient.
    recipient->CopyFirstFrom(last_value, buffer_pool_manager);
    recipient->SetKeyAt(1, middle_key);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    // Make space for the new entry.
    for (int i = GetSize(); i > 0; i--) {
        SetKeyAt(i, KeyAt(i - 1));
        SetValueAt(i, ValueAt(i - 1));
    }

    // Add the value to the first position.
    SetValueAt(0, value);

    // Update the parent page id for the child.
    auto child_page = buffer_pool_manager->FetchPage(value);
    if (child_page != nullptr) {
        auto child_bPlusTree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child_bPlusTree_page->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(value, true);
    }

    SetSize(GetSize() + 1);
}