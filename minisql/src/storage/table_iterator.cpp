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
    ASSERT(rid_.GetPageId() == INVALID_PAGE_ID, "Invalid page!");
    ASSERT(rid_ == INVALID_ROWID, "Invalid row id!");
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
TableIterator &TableIterator::operator++() {
    if (table_heap_ == nullptr) { return *this; } // Reach end();

    rid_ = table_heap_->GetNextRowId(rid_, txn_); // Use TableHeap::GetNextRowId().
    auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
    page->RLatch();
    page->GetTuple(&row_, table_heap_->schema_, txn_, table_heap_->lock_manager_);
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);
    return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
    TableIterator result(*this);
    ++(*this);
    return result; // `return` will use copy constructor, which can't be defined as explicit.
}
