#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
    /**
     * Use First Fit strategy, try to find the first TablePage that is able to hold the record.
     * `RowId` should be returned through `row.rid_`. (Done by TablePage::InsertTuple) 
     */
    
    // Make sure the tuple is not too large.
    uint32_t tuple_size = row.GetSerializedSize(schema_);
    ASSERT(tuple_size < PAGE_SIZE, "Tuple size is larger than PAGE_SIZE!");

    // Insert the tuple using First Fit.
    page_id_t curPageId = first_page_id_;
    bool isInserted = false;
    while (!isInserted) {
        Page *cur_Page = buffer_pool_manager_->FetchPage(curPageId);
        if (cur_Page == nullptr) {
            // throw std::runtime_error("Page ID invalid!");
            return false;
        }
        auto curPage = reinterpret_cast<TablePage *>(cur_Page);
        
        curPage->WLatch();
        isInserted = curPage->InsertTuple(row, schema_, txn, lock_manager_, log_manager_); // Try to insert the tuple to an old page.
        curPage->WUnlatch();
        buffer_pool_manager_->UnpinPage(curPage->GetPageId(), isInserted);

        page_id_t nextPageId = curPage->GetNextPageId();
        if (isInserted) { break; }
        else if (nextPageId != INVALID_PAGE_ID) { curPageId = nextPageId; }
        else {
            // No place for the tuple in old pages. Need to attach a new page.
            page_id_t newPageId;
            Page *new_page = buffer_pool_manager_->NewPage(newPageId);
            if (new_page == nullptr) {
                // throw std::runtime_error("Can't get a new page!");
                return false;
            }
            auto newPage = reinterpret_cast<TablePage *>(new_page);
            
            // Initialize the new page and attach it to the last page.
            newPage->WLatch();
            newPage->Init(newPageId, curPageId, log_manager_, txn);
            newPage->WUnlatch();
            curPage->WLatch();
            curPage->SetNextPageId(newPageId);
            curPage->WUnlatch();
            buffer_pool_manager_->UnpinPage(curPageId, true);

            // Insert the tuple.
            newPage->WLatch();
            isInserted = newPage->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
            newPage->WUnlatch();
            buffer_pool_manager_->UnpinPage(newPageId, true);
            
            break;
        }
    }

    return isInserted;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
    /**
     * Replace old row with new row where `RowId` is equal to `rid`.
     * `RowId` should be returned through `new_row.rid_`.
     */
    Page *old_Page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    if (old_Page == nullptr) {
        // throw std::runtime_error("Old page not found!");
        return false;
    }
    auto oldPage = reinterpret_cast<TablePage *>(old_Page);

    // TablePage::UpdateTuple requires a old_row.
    // Row *old_row;
    Row old_row; // To avoid wild pointer!
    old_row.SetRowId(rid); // IMPORTANT!
    oldPage->WLatch();
    if (!(oldPage->GetTuple(&old_row, schema_, txn, lock_manager_))) {
        // throw std::runtime_error("Can't get tuple from the old page!");
        old_Page->WUnlatch();
        return false;
    }
    // Update the tuple.
    bool isUpdated = oldPage->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
    oldPage->WUnlatch();
    if (!isUpdated) { // New row is too large to be updated. Remove the old row and insert the new row again.
        MarkDelete(rid, txn);
        bool isInserted = InsertTuple(row, txn);
        if (isInserted) // `RowId` will be returned using InsertTuple.
            RollbackDelete(rid, txn);
        return isInserted;
    }
    buffer_pool_manager_->UnpinPage(oldPage->GetPageId(), isUpdated);  
    row.SetRowId(rid);  
    return isUpdated;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
    /**
     * Step1: Find the page which contains the tuple.
     * Step2: Delete the tuple from the page.
     * */ 
    Page *page_ = buffer_pool_manager_->FetchPage(rid.GetPageId());
    if (page_ == nullptr) {
        // throw std::runtime_error("Page not found!");
        return;
    }
    auto page = reinterpret_cast<TablePage *>(page_);

    // Apply delete.
    page->WLatch();
    page->ApplyDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
    Page *page_ = buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId());
    if (page_ == nullptr) {
        // throw std::runtime_error("Page not found!");
        return false;
    }
    auto page = reinterpret_cast<TablePage *>(page_);

    page->RLatch();
    bool isGetTuple = page->GetTuple(row, schema_, txn, lock_manager_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false); // Unpinned here.
    return isGetTuple;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */

TableIterator TableHeap::Begin(Txn *txn) { // Need to match up with end()!
    page_id_t current_page_id = first_page_id_;
    RowId first_valid_rid;
    bool found_first_tuple = false;

    // Try to find the first valid tuple
    while (current_page_id != INVALID_PAGE_ID) {
        Page* page = buffer_pool_manager_->FetchPage(current_page_id);
        if (page == nullptr) {
            LOG(ERROR) << "TableHeap::Begin: Failed to fetch page " << current_page_id << ". Aborting scan.";
            return End(); 
        }
        
        auto table_page = reinterpret_cast<TablePage*>(page->GetData());
        table_page->RLatch();
        found_first_tuple = table_page->GetFirstTupleRid(&first_valid_rid);
        table_page->RUnlatch();

        page_id_t next_page_id = table_page->GetNextPageId(); 
        buffer_pool_manager_->UnpinPage(current_page_id, false);

        if (found_first_tuple) {
            return TableIterator(this, first_valid_rid, txn);
        }

        current_page_id = next_page_id;
    }
    return End(); 
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { 
    return TableIterator(nullptr, RowId(), nullptr);
}

/**
 * Get next valid row id.
 */

/**
 * Some fatal problems in TableHeap::GetNextRowId()!
 */

// RowId TableHeap::GetNextRowId(RowId rid, Txn *txn) {
//     RowId next_rid;
//     auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//     if (page == nullptr)
//         return INVALID_ROWID;

//     // Search from this page.
//     bool isFound = false;
//     page->RLatch();
//     isFound = page->GetNextTupleRid(rid, &next_rid);
//     page->RUnlatch();
//     buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
//     if (isFound) {
//         return next_rid;
//     }

//     // Search from the others.
//     auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
//     if (next_page == nullptr)
//         return INVALID_ROWID;
//     while (!isFound) {
//         next_page->RLatch();
//         isFound = next_page->GetFirstTupleRid(&next_rid);
//         next_page->RUnlatch();
//         buffer_pool_manager_->UnpinPage(next_page->GetPageId(), false);
//         if (isFound) { return next_rid; } // Found in this page.
//         // Countinue.
//         next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page->GetNextPageId()));
//         if (next_page == nullptr) // Reach the end().
//             return INVALID_ROWID;        
//     }

//     throw std::runtime_error("Can't get a proper next id!");
// }
