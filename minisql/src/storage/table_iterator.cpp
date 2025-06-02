#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

// TableIterator::TableIterator() {};

/**
 * TODO: Student Implement
 */    

 // Default constructor.
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) : table_heap_(table_heap), rid_(rid), txn_(txn) {
    if (table_heap_ != nullptr && rid_.GetPageId() != INVALID_PAGE_ID) {
        auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
        if (page == nullptr) {
            throw std::runtime_error("Invalid page!");
        } else {
            row_.SetRowId(rid_);
            page->RLatch();
            bool isGetTuple = page->GetTuple(&row_, table_heap_->schema_, txn, table_heap_->lock_manager_);
            page->RUnlatch();
            table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);
            if (!isGetTuple)
                throw std::runtime_error("Invalid rid!");
        }
    }
}

// Copy constructor.
TableIterator::TableIterator(const TableIterator &other)
    : table_heap_(other.table_heap_), rid_(other.rid_), txn_(other.txn_), row_(other.row_) {}

// Default destructor.
TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator &itr) const {
    return (table_heap_ == itr.table_heap_) && (rid_ == itr.rid_);
    // return (table_heap_ == itr.table_heap_) && (rid_ == itr.rid_) && (row_ == itr.row_);
}

bool TableIterator::operator!=(const TableIterator &itr) const {
    return !((table_heap_ == itr.table_heap_) && (rid_ == itr.rid_));
    // return !((table_heap_ == itr.table_heap_) && (rid_ == itr.rid_) && (row_ == itr.row_));
}

const Row &TableIterator::operator*() {
    /**
     * Return the row.
     */
    // ASSERT(table_heap_ != nullptr, "Table heap is empty!");
    // Row *row = new Row(rid_); // [error] Memory leaks orrcur here.
    // table_heap_->GetTuple(row, txn_);
    // ASSERT(row != nullptr, "Can't get the row!");
    // return *row;
    ASSERT(table_heap_ != nullptr, "Table heap is empty!");
    ASSERT(rid_.GetPageId() != INVALID_PAGE_ID, "Invalid page!");
    // ASSERT(rid_ != INVALID_ROWID, "Invalid row id!");
    return row_;
}

Row *TableIterator::operator->() {
    /**
     * Return the row.
     */
    // ASSERT(table_heap_ != nullptr, "Table heap is empty!");
    // Row *row = new Row(rid_); // [error] Memory leaks orrcur here.
    // table_heap_->GetTuple(row, txn_);
    // ASSERT(row != nullptr, "Can't get the row!");
    // return row;
    ASSERT(table_heap_ != nullptr, "Table heap is empty!");
    ASSERT(rid_.GetPageId() != INVALID_PAGE_ID, "Invalid page!");
   // ASSERT(rid_ != INVALID_ROWID, "Invalid row id!");
    return &row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    table_heap_ = itr.table_heap_;
    rid_ = itr.rid_;
    txn_ = itr.txn_;
    row_ = itr.row_;
    return *this;
}

// ++iter
// TableIterator &TableIterator::operator++() {
//     if (table_heap_ == nullptr) { return *this; } // Reach end();

//     rid_ = table_heap_->GetNextRowId(rid_, txn_); // Use TableHeap::GetNextRowId().
//     auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
//     page->RLatch();
//     page->GetTuple(&row_, table_heap_->schema_, txn_, table_heap_->lock_manager_);
//     page->RUnlatch();
//     table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);
//     return *this;
// }

// TableIterator &TableIterator::operator++() {
//     if (table_heap_ == nullptr) {
//         return *this;
//     }
//     if (rid_.GetPageId() == INVALID_PAGE_ID) {
//         table_heap_ = nullptr;
//         return *this;
//     }

//     while (true) {
//         rid_ = table_heap_->GetNextRowId(rid_, txn_);
//         if (rid_.GetPageId() == INVALID_PAGE_ID) {
//             table_heap_ = nullptr;
//             return *this;
//         }

//         auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));

//         if (page == nullptr) {
//             rid_ = RowId(INVALID_PAGE_ID, 0);
//             table_heap_ = nullptr;
//             return *this;
//         }

//         bool get_tuple_success = false;
//         page->RLatch();
//         get_tuple_success = page->GetTuple(&row_, table_heap_->schema_, txn_, table_heap_->lock_manager_);
//         page->RUnlatch();
//         table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);

//         if (get_tuple_success) {
//             row_.SetRowId(rid_); 
//             return *this;
//         }
//     }
// }

TableIterator &TableIterator::operator++() {
    if (table_heap_ == nullptr) {
        return *this;
    }
    if (rid_.GetPageId() == INVALID_PAGE_ID) {
        table_heap_ = nullptr;
        return *this;
    }

    RowId next_rid; // Use a temporary variable for the next rid found

    // Try to find a valid slot in this page
    Page* current_page = table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId());

    bool found_next_tuple_rid = false;
    if (current_page) {
        auto current_table = reinterpret_cast<TablePage *>(current_page->GetData());
        current_table->RLatch();
        found_next_tuple_rid = current_table->GetNextTupleRid(rid_, &next_rid);
        current_table->RUnlatch();

        page_id_t next_page_id = current_table->GetNextPageId(); // Get next page id before unpining the current one
        table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);

        if (!found_next_tuple_rid) {
            // If not found on the current page, start scan from the beginning of the next page
            page_id_t cur_page_id = next_page_id;
            while (cur_page_id != INVALID_PAGE_ID) {
                Page* cur_page = table_heap_->buffer_pool_manager_->FetchPage(cur_page_id);
                auto cur_table = reinterpret_cast<TablePage *>(cur_page->GetData());
                
                RowId first_rid;
                cur_table->RLatch();
                // GetFirstTupleRid finds the first valid tuple on this new page
                bool found_first_tuple_rid = cur_table->GetFirstTupleRid(&first_rid);
                cur_table->RUnlatch();

                page_id_t next_page_id_tmp = cur_table->GetNextPageId(); // Get next before unpinning
                table_heap_->buffer_pool_manager_->UnpinPage(cur_page_id, false);

                if (found_first_tuple_rid) {
                    next_rid = first_rid;
                    found_next_tuple_rid = true;
                    break;
                }
                next_page_id = next_page_id_tmp;
            }
        }
    } else {
        LOG(ERROR) << "TableIterator::operator++: Failed to fetch current page " << rid_.GetPageId() << ". Iterator becomes end.";
        found_next_tuple_rid = false;
    }


    // Not found
    if (!found_next_tuple_rid) {
        table_heap_ = nullptr;
        rid_ = RowId(); // Set to default invalid RowId.
        // txn_ = nullptr;
        return *this;
    }

    // Found
    rid_ = next_rid;
    row_.destroy(); // Clear existing fields
    row_.SetRowId(rid_);

    Page* new_data_page = table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId());
    if (new_data_page) {
        auto new_data_table = reinterpret_cast<TablePage *>(new_data_page->GetData());
        new_data_table->RLatch();
        bool get_tuple = new_data_table->GetTuple(&row_, table_heap_->schema_, txn_, table_heap_->lock_manager_);
        new_data_table->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);

        if (!get_tuple) {
            table_heap_ = nullptr;
            rid_ = RowId();
            // txn_ = nullptr;
            return *this;
        }
    } else {
        // Failed to fetch page for the new rid_
        LOG(ERROR) << "TableIterator::operator++: Failed to fetch page " << rid_.GetPageId()
                   << " for the new valid rid. Iterator becomes end.";
        table_heap_ = nullptr;
        rid_ = RowId();
        // txn_ = nullptr;
        return *this;
    }
    return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
    TableIterator result(*this);
    ++(*this);
    return result; // `return` will use copy constructor, which can't be defined as explicit.
}