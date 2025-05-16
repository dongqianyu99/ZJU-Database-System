#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

// TableIterator::TableIterator() {};

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) : table_heap_(table_heap), rid_(rid), txn_(txn) {
    /**
     * Default constructor.
     */
}

// TableIterator::TableIterator(const TableIterator &other) {
//     /**
//      * Copy constructor.
//      */
//     table_heap_ = other.table_heap_;
//     rid_ = other.rid_;
//     txn_ = other.txn_;
// }

TableIterator::TableIterator(const TableIterator &other)
    : table_heap_(other.table_heap_), rid_(other.rid_), txn_(other.txn_) {
    // Body can be empty if all initialization is done in the list
}

TableIterator::~TableIterator() {
    /**
     * Default destructor.
     */
}

bool TableIterator::operator==(const TableIterator &itr) const {
    return (table_heap_ == itr.table_heap_) && (rid_ == itr.rid_);
}

bool TableIterator::operator!=(const TableIterator &itr) const {
    return !((table_heap_ == itr.table_heap_) && (rid_ == itr.rid_));
}

const Row &TableIterator::operator*() {
    /**
     * Return the row.
     */
    ASSERT(table_heap_ != nullptr, "Table heap is empty!");
    Row *row = new Row(rid_);
    table_heap_->GetTuple(row, txn_);
    ASSERT(row != nullptr, "Can't get the row!");
    return *row;
}

Row *TableIterator::operator->() {
    /**
     * Return the row.
     */
    ASSERT(table_heap_ != nullptr, "Table heap is empty!");
    Row *row = new Row(rid_);
    table_heap_->GetTuple(row, txn_);
    ASSERT(row != nullptr, "Can't get the row!");
    return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    table_heap_ = itr.table_heap_;
    rid_ = itr.rid_;
    txn_ = itr.txn_;
    return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
    if (table_heap_ == nullptr) { return *this; } // Reach end();

    rid_ = table_heap_->GetNextRowId(rid_, txn_); // Use TableHeap::GetNextRowId().
    return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
    TableIterator result(*this);
    ++(*this);
    return result; // `return` will use copy constructor, which can't be defined as explicit.
}
