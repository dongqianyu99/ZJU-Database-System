#include "index/b_plus_tree_index.h"

#include <string>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/generic_key.h"

static const std::string db_name = "bp_tree_index_test.db";

TEST(BPlusTreeTests, BPlusTreeIndexGenericKeyTest) {
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<uint32_t> index_key_map{0, 1};
  const TableSchema table_schema(columns);
  auto *key_schema = Schema::ShallowCopySchema(&table_schema, index_key_map);
  std::vector<Field> fields{Field(TypeId::kTypeInt, 27),
                            Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)};
  KeyManager KP(key_schema, 128);
  Row key(fields);
  GenericKey *k1 = KP.InitKey();
  KP.SerializeFromKey(k1, key, key_schema);
  GenericKey *k2 = KP.InitKey();
  Row copy_key(fields);
  KP.SerializeFromKey(k2, copy_key, key_schema);
  ASSERT_EQ(0, KP.CompareKeys(k1, k2));
}

TEST(BPlusTreeTests, BPlusTreeIndexSimpleTest) {
  auto disk_mgr_ = new DiskManager(db_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  page_id_t id;
  if (bpm_->IsPageFree(CATALOG_META_PAGE_ID)) {
    if (bpm_->NewPage(id) == nullptr || id != CATALOG_META_PAGE_ID) {
      throw logic_error("Failed to allocate catalog meta page.");
    }
  }
  if (bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID)) {
    if (bpm_->NewPage(id) == nullptr || id != INDEX_ROOTS_PAGE_ID) {
      throw logic_error("Failed to allocate header page.");
    }
  }
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<uint32_t> index_key_map{0, 1};
  const TableSchema table_schema(columns);
  auto *index_schema = Schema::ShallowCopySchema(&table_schema, index_key_map);
  auto *index = new BPlusTreeIndex(0, index_schema, 256, bpm_);
  for (int i = 0; i < 10; i++) {
    std::vector<Field> fields{Field(TypeId::kTypeInt, i),
                              Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)};
    Row row(fields);
    RowId rid(1000, i);
    ASSERT_EQ(DB_SUCCESS, index->InsertEntry(row, rid, nullptr));
  }

  // Test Scan
  std::vector<RowId> ret;
  for (int i = 0; i < 10; i++) {
    std::vector<Field> fields{Field(TypeId::kTypeInt, i),
                              Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)};
    Row row(fields);
    RowId rid(1000, i);
    ASSERT_EQ(DB_SUCCESS, index->ScanKey(row, ret, nullptr));
    ASSERT_EQ(rid.Get(), ret[i].Get());
  }
  // Iterator Scan
  IndexIterator iter = index->GetBeginIterator();
  uint32_t i = 0;
  for (; iter != index->GetEndIterator(); ++iter) {
    ASSERT_EQ(1000, (*iter).second.GetPageId());
    ASSERT_EQ(i, (*iter).second.GetSlotNum());
    i++;
  }
  ASSERT_EQ(10, i);
  index->Destroy();
  delete index;
  delete bpm_;
  delete disk_mgr_;
}