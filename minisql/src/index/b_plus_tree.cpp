#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM, int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {

    // Default size.
    if (leaf_max_size_ == UNDEFINED_SIZE) {
        leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId));
    }
    if (internal_max_size_ == UNDEFINED_SIZE) {
        internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(page_id_t));
    }

    // Load root_page_id_ from header page.
    Page *root_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
    if (root_page == nullptr) {
        LOG(WARNING) << "Faied to fatch index roots page.";
    } else {
        auto *root_node = reinterpret_cast<IndexRootsPage *>(root_page->GetData());
        root_node->GetRootId(index_id_, &root_page_id_);
    }
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
    // If called with default, destroy the whole tree.
    if (current_page_id == INVALID_PAGE_ID)
        if (root_page_id_ == INVALID_FRAME_ID) { return; } // The tree is already destroyed.
        current_page_id = root_page_id_;

    auto *node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
    if (node == nullptr) { return; }
    
    // If the node is an internal node, delete recursively.
    if (!node->IsLeafPage()) {
        InternalPage *internal_node = static_cast<InternalPage *>(node);
        for (int i = 0; i < internal_node->GetSize(); i++) {
            Destroy(internal_node->ValueAt(i));
        }
    }

    buffer_pool_manager_->UnpinPage(current_page_id, true);
    buffer_pool_manager_->DeletePage(current_page_id);

    if (current_page_id == root_page_id_) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId(); // Call this method everytime root page id is changed.
    }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
    if (IsEmpty()) { return false; }
    auto *leaf_node = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_, false)->GetData());

    // Invalid key.
    if (leaf_node == nullptr) { return false; }

    // Look up in the leaf node.
    RowId value;
    bool found = leaf_node->Lookup(key, value, processor_);

    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);

    if (found) {
        result.push_back(value);
    }
    return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
    if (IsEmpty()) {
        StartNewTree(key, value);
        return true;
    }
    return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
    page_id_t new_page_id;
    Page *root_page = buffer_pool_manager_->NewPage(new_page_id);
    
    if (root_page == nullptr) { throw std::runtime_error("Out of memory error! Can't allocate a new page!"); }
    root_page_id_ = new_page_id;
    UpdateRootPageId(1);
    
    auto *root_node = reinterpret_cast<LeafPage *>(root_page->GetData());
    root_node->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
    root_node->Insert(key, value, processor_);
    
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
    Page *leaf_page = FindLeafPage(key, root_page_id_, false);
    if (leaf_page == nullptr) { return false; } // Not found.
    auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

    // Check for duplications.
    RowId val;
    if (leaf_node->Lookup(key, val, processor_)) {
       buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
       return false;
    }

    // If the leaf node has extra space.
    if (leaf_node->GetSize() < leaf_node->GetMaxSize()) {
        leaf_node->Insert(key, value, processor_);
        buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);  

        // std::cout << "Insert. Not spliting." << endl;

    } else { // If not, need to split the node.
        // std::cout << "Spliting." << endl;

        auto *new_leaf_node = Split(leaf_node, transaction); // Larger half is moved to the now node.
        GenericKey *middle_key = new_leaf_node->KeyAt(0);

        if (processor_.CompareKeys(key, middle_key) < 0) { // key < middle_key, insert into the old half.
            leaf_node->Insert(key, value, processor_);
        } else { // key > middle_key, insert into the new half.
            new_leaf_node->Insert(key, value, processor_);
        }

        InsertIntoParent(leaf_node, middle_key, new_leaf_node, transaction); // Set to the parent node.
        buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);
    }

    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
    page_id_t new_page_id;
    Page *new_internal_page = buffer_pool_manager_->NewPage(new_page_id);
    if (new_internal_page == nullptr) { throw std::runtime_error("Out of memory error! Can't allocate a new page for split!"); }
    auto *new_internal_node = reinterpret_cast<InternalPage *>(new_internal_page->GetData());
    new_internal_node->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
    node->MoveHalfTo(new_internal_node, buffer_pool_manager_);

    // Unpinned in the BPlusTree::InsertIntoParent()
    return new_internal_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
    page_id_t new_page_id;
    Page *new_leaf_page = buffer_pool_manager_->NewPage(new_page_id);
    if (new_leaf_page == nullptr) { throw std::runtime_error("Out of memory error! Can't allocate a new page for split!"); }
    auto *new_leaf_node = reinterpret_cast<LeafPage *>(new_leaf_page->GetData());
    new_leaf_node->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
    node->MoveHalfTo(new_leaf_node);

    new_leaf_node->SetNextPageId(node->GetNextPageId());
    node->SetNextPageId(new_page_id);

    // Upinned in the BPlusTree::InsertIntoLeaf()
    return new_leaf_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
    if (old_node->IsRootPage()) { // If the node to be splitted is the root node, create a new root node.
        page_id_t new_root_page_id;
        Page *new_root_page = buffer_pool_manager_->NewPage(new_root_page_id);
        if (new_root_page == nullptr) { throw std::runtime_error("Out of memory error! Can't allocate a new page for split!"); }
        auto *new_root_node = reinterpret_cast<InternalPage *>(new_root_page->GetData());
        new_root_node->Init(new_root_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);

        // Populate new root page with old_value + new_key & new_value
        new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

        old_node->SetParentPageId(new_root_page_id);
        new_node->SetParentPageId(new_root_page_id);

        root_page_id_ = new_root_page_id;
        UpdateRootPageId();

        buffer_pool_manager_->UnpinPage(new_root_page_id, true);
        return;
    }

    // The node to be splitted is the root node.
    page_id_t parent_page_id = old_node->GetParentPageId();
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

    if (parent_node->GetSize() < parent_node->GetMaxSize()) { // No need to continue splitting.
        parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(parent_page_id);
        buffer_pool_manager_->UnpinPage(parent_page_id, true);
    } else { // Parent is full, split.
        InternalPage *new_parent_node_sibling = Split(parent_node, transaction);
        GenericKey *middle_key = new_parent_node_sibling->KeyAt(0);
        if (processor_.CompareKeys(key, middle_key) < 0) {
            parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());new_node->SetParentPageId(parent_node->GetPageId());
        } else {
            new_parent_node_sibling->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
            new_node->SetParentPageId(new_parent_node_sibling->GetPageId());
        }
        
        // Recursively splitting upwards if needed.
        InsertIntoParent(parent_node, middle_key, new_parent_node_sibling, transaction);

        buffer_pool_manager_->UnpinPage(parent_page_id, true);
        buffer_pool_manager_->UnpinPage(new_parent_node_sibling->GetPageId(), true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
    if (IsEmpty()) { return; }

    // Find the right leaf page as deletion target.
    Page *leaf_page = FindLeafPage(key, root_page_id_, false);
    if (leaf_page == nullptr) { return; }
    auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

    int size_before_delete = leaf_node->GetSize();
    
    // Get key index.
    int key_index = leaf_node->KeyIndex(key, processor_);
    if (processor_.CompareKeys(key, leaf_node->KeyAt(key_index)) != 0) { // Not found.
        buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
        return; 
    }

    // Remove key_index.
    leaf_node->RemoveAndDeleteRecord(key, processor_);

    bool unpin_dirty = true; // Mark if the page still needs unpinning.

    // Redistribute or merge(coalesce).
    if (leaf_node->GetSize() < leaf_node->GetMinSize()) {
        // The BPlusTree::CoalesceOrRedistribute() function is supposed to handle unpinning of the node it processes/deletes.
        bool node_deleted = CoalesceOrRedistribute(leaf_node, transaction);
        if (node_deleted) { unpin_dirty = false; }
    }

    // Make sure the page is unpinned.
    if (unpin_dirty && leaf_page->GetPageId() == leaf_node->GetPageId()) 
        buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    else if (!unpin_dirty) {}  // The page is already handled. 
    else // Not supposed to get here.
        buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);

    // Check if root has became empty.  
    if (!IsEmpty()) {
        Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
        auto *root_node = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
        if (root_node->IsLeafPage() && root_node->GetSize() == 0) {
            buffer_pool_manager_->UnpinPage(root_page_id_, false);
            buffer_pool_manager_->DeletePage(root_page_id_);
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId();
        } else {
            buffer_pool_manager_->UnpinPage(root_page_id_, false);
        }
    }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
    /**
     * 1. If the node is smaller than the minimum size(underflow):
     *    - Try to borrow from siblings(Redistribute);
     *    - If can't, merge the node with its sibiling(Coalesce).
     * 2. If Coalesce causes underflowing in parent node, recursively handling parent nodes.
     */
    if (node->IsRootPage()) { return AdjustRoot(node); }

    Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    // Get the index in its parent node.
    int index_in_parent = parent_node->ValueIndex(node->GetPageId());

    // Redistribute
    // Try to find left sibling.
    N *left_sibling_node = nullptr;
    Page *left_sibling_page = nullptr;
    if (index_in_parent > 0) { // Not the first one -> must have a left sibling.
        page_id_t left_sibling_page_id = parent_node->ValueAt(index_in_parent - 1);
        left_sibling_page = buffer_pool_manager_->FetchPage(left_sibling_page_id);
        left_sibling_node = reinterpret_cast<N *>(left_sibling_page->GetData());

        if (left_sibling_node->GetSize() > left_sibling_node->GetMinSize()) { // At least one element can be borrowed from the left sibling node.
            /**
             * keys_:     [      K1     K2     K3       ]
             * values_:   [  P0     P1     P2     P3    ]
             *               ↑      ↑      ↑      ↑
             *           idx=0      1      2      3
             * When redistribute with left sibling, use index_in_parent; ortherwise, use 
             * index_in_parent + 1.
             */
            // Redistribute(left_sibling_node, node, index_in_parent);
            Redistribute(left_sibling_node, node, 1);
            buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
            return false; // Not need for deleting node.
        }
    }

    // Try to find right sibling.
    N *right_sibling_node = nullptr;
    Page *right_sibling_page = nullptr;
    if (index_in_parent < parent_node->GetSize() - 1) { // Not the last one -> must have a right sibling.
        page_id_t right_sibling_page_id = parent_node->ValueAt(index_in_parent + 1);
        right_sibling_page = buffer_pool_manager_->FetchPage(right_sibling_page_id);
        right_sibling_node = reinterpret_cast<N *>(right_sibling_page->GetData());

        if (right_sibling_node->GetSize() > right_sibling_node->GetMinSize()) { // At least one element can be borrowed from the right sibling node.
            // Redistribute(right_sibling_node, node, index_in_parent + 1);
            Redistribute(right_sibling_node, node, 0);
            buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
            if (left_sibling_page) // If fechted but not used, unpin it.
                buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), false);
            return false; // Not need for deleting node.
        }
    }

    // Coalesce
    bool coalesce_with_left = false;
    if (left_sibling_node) { // Coalesce with left sibling.
        Coalesce(left_sibling_node, node, parent_node, index_in_parent, transaction);
        buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
        if (right_sibling_page) 
            buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), false);
        return true; // Merge this node into the left sibling.
    } else if (right_sibling_node) {  // Coalesce with right sibling.(only the first node)
        Coalesce(node, right_sibling_node, parent_node, index_in_parent + 1, transaction);
        buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
        return false;
    } else { // Should not happen.
        if (left_sibling_page) 
            buffer_pool_manager_->UnpinPage(left_sibling_page->GetPageId(), false);
        if (right_sibling_page)
            buffer_pool_manager_->UnpinPage(right_sibling_page->GetPageId(), false);
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
        return false; // Merge the right sibling into this node.
    }

    return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
    node->MoveAllTo(neighbor_node);
    neighbor_node->SetNextPageId(node->GetNextPageId());
    parent->Remove(index);
    buffer_pool_manager_->DeletePage(node->GetPageId());

    // If parent node underflows.
    if (parent->GetSize() < parent->GetMinSize()) {
        return CoalesceOrRedistribute<BPlusTree::InternalPage>(parent, transaction);
    } else {
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
        return false;
    }
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
    GenericKey *key_from_parent = parent->KeyAt(index);
    node->MoveAllTo(neighbor_node, key_from_parent, buffer_pool_manager_);
    parent->Remove(index);
    buffer_pool_manager_->DeletePage(node->GetPageId());

    // If parent node underflows.                       
    if (parent->GetSize() < parent->GetMinSize()) {
        return CoalesceOrRedistribute<BPlusTree::InternalPage>(parent, transaction);
    } else {
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
        return false;
    }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
    Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

    if (index != 0) { // Sibling is left node.
        neighbor_node->MoveLastToFrontOf(node);
        parent_node->SetKeyAt(parent_node->ValueIndex(node->GetPageId()), node->KeyAt(0));
    } else { // Sibling is right node.
        neighbor_node->MoveFirstToEndOf(node);
        parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
    }
    // Unpinned by the caller.
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
    Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

    if (index != 0) {  // Sibling is left node.
        int middle_key_index = parent_node->ValueIndex(node->GetPageId());
        neighbor_node->MoveLastToFrontOf(node,
                                         parent_node->KeyAt(middle_key_index),
                                         buffer_pool_manager_);
    } else {  // Sibling is right node.
        int middle_key_index = parent_node->ValueIndex(neighbor_node->GetPageId());
        neighbor_node->MoveFirstToEndOf(node,
                                        parent_node->KeyAt(middle_key_index),
                                        buffer_pool_manager_);
    }
    // Unpinned by the caller.
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
    if (old_root_node->IsLeafPage()) {
        if (old_root_node->GetSize() == 0) {
            buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId();
            return true; // Root has deleted.
        }
    } else {
        /**
         * Internal root case, where for a internal node, there is no key left but a child.
         * In this case, we need to set the child as the new root.
         */
        auto *internal_root_node = reinterpret_cast<InternalPage *>(old_root_node->GetPageId());
        if (internal_root_node->GetSize() == 0) {
            root_page_id_ = internal_root_node->ValueAt(0); // The child becomes the new root.
            UpdateRootPageId();
            // Set the parent_id of the new root as INVALID_PAGE_ID.
            Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
            auto *new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
            new_root_node->SetParentPageId(INVALID_PAGE_ID);
            buffer_pool_manager_->UnpinPage(root_page_id_, true);
            // Delete the old one.
            buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
            return true; // Root has deleted.
        }
    }

    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
    if (IsEmpty()) { return End(); }
    Page *leaf_page = FindLeafPage(nullptr, root_page_id_, true);
    if (leaf_page == nullptr) { return End(); }

    // Construct the iterator.
    IndexIterator iter(leaf_page->GetPageId(), buffer_pool_manager_, 0);

    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    
    return iter;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
    if (IsEmpty()) { return End(); }
    Page *leaf_page = FindLeafPage(key, root_page_id_, true);
    if (leaf_page == nullptr) { return End(); }
    auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
    int key_index = leaf_node->KeyIndex(key, processor_);

    // Construct the iterator.
    IndexIterator iter(leaf_page->GetPageId(), buffer_pool_manager_, key_index);

    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

    return iter;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
    if (IsEmpty()) { return nullptr; }
    
    // If page_id is not provided or invalid, use root_page_id_.
    page_id_t cur_page_id = (page_id == INVALID_PAGE_ID || page_id == 0) ? root_page_id_ : page_id;
    if (cur_page_id == INVALID_PAGE_ID) { return nullptr; }

    Page *page = buffer_pool_manager_->FetchPage(cur_page_id);
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

    while (!node->IsLeafPage()) {
        InternalPage *internal_node = static_cast<InternalPage *>(node);
        page_id_t page_for_upin = internal_node->GetPageId(); // Record page id for unpining.

        if (leftMost) {
            cur_page_id = internal_node->ValueAt(0); // Go to the leftest node.
        } else {
            cur_page_id = internal_node->Lookup(key, processor_); // Find the wanted node.
        }

        buffer_pool_manager_->UnpinPage(page_for_upin, false);
        if (cur_page_id == INVALID_PAGE_ID) { return nullptr; } // Should not happend.
        page = buffer_pool_manager_->FetchPage(cur_page_id);
        node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    }
    // Unpinned by the caller.
    return page;
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page is defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
    Page *header_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
    if (header_page == nullptr) {
        throw std::runtime_error("Unable to fetch INDEX_ROOTS_PAGE_ID");
    }
    auto *index_roots_page = reinterpret_cast<IndexRootsPage *>(header_page->GetData());

    if (insert_record == 1) // Insert a new root.
        index_roots_page->Insert(index_id_, root_page_id_);
    else if (root_page_id_ == INVALID_PAGE_ID) // Tree deleted.
        index_roots_page->Delete(index_id_);
    else // Root page_id changed.
        index_roots_page->Update(index_id_, root_page_id_);

    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}