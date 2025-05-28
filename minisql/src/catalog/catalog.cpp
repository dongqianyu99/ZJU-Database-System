#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
    return sizeof(uint32_t) + // magic number
           sizeof(uint32_t) + // table_meta_pages_ count
           sizeof(uint32_t) + // index_meta_pages_ count
           table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t)) +
           index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), 
      lock_manager_(lock_manager), 
      log_manager_(log_manager),
      catalog_meta_(nullptr),
      next_table_id_(0),
      next_index_id_(0) { 
    LOG(INFO) << "CatalogManager: Initializing.";
    if (init) { // New database
        catalog_meta_ = CatalogMeta::NewInstance();
        if (FlushCatalogMetaPage() != DB_SUCCESS) {
            LOG(FATAL) << "Failed to flush initial catalog metadata page.";
            delete catalog_meta_;
            catalog_meta_ = nullptr;
        }
        LOG(INFO) << "CatalogManager: Initialized new catalog metadata.";
    } else { // Existing database
        Page *meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
        if (meta_page == nullptr) { 
            LOG(FATAL) << "Failed to fetch catalog meta page " << CATALOG_META_PAGE_ID << ".";
        }
        catalog_meta_ = CatalogMeta::DeserializeFrom(meta_page->GetData());
        buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
        if (catalog_meta_ == nullptr) { 
            LOG(FATAL) << "Failed to deserialize catalog metadata from page " << CATALOG_META_PAGE_ID << ".";
        }

        // Load all tables
        for (const auto &pair : catalog_meta_->table_meta_pages_) {
            if (LoadTable(pair.first, pair.second) != DB_SUCCESS) {
                LOG(ERROR) << "Failed to load table id " << pair.first << " from page " << pair.second <<".";
            }
        }

        // Load all indexes
        for (const auto &pair : catalog_meta_->index_meta_pages_) {
            if (LoadIndex(pair.first, pair.second) != DB_SUCCESS) {
                LOG(ERROR) <<  "Failed to load index id " << pair.first << " from page " << pair.second <<".";
            }
        }

        next_table_id_.store(catalog_meta_->GetNextTableId());
        next_index_id_.store(catalog_meta_->GetNextIndexId());
        LOG(INFO) << "CatalogManager: Catalog loaded.";
    }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
    LOG(INFO) << "CatalogManager: CreateTable Attempt: " << table_name;
    // Check for duplication
    if (table_names_.count(table_name)) {
        LOG(ERROR) << "CatalogManager: CreateTable Failed - Table already exists: " << table_name;
        return DB_TABLE_ALREADY_EXIST;
    }

    table_id_t new_table_id = next_table_id_.fetch_add(1);
    page_id_t meta_page_id;
    Page *table_meta_page = buffer_pool_manager_->NewPage(meta_page_id);
    if (table_meta_page == nullptr) {
        LOG(ERROR) << "CatalogManager: CreateTable Failed - Cannot allocate page for table metadata for '" << table_name << "'.";
        next_table_id_.fetch_sub(1); // Rollback ID
        return DB_FAILED;
    }

    try {
        TableMetadata *table_meta = TableMetadata::Create(new_table_id, table_name, INVALID_PAGE_ID, schema);
        if (!table_meta) {
            throw std::runtime_error("TableMetadata::Create failed to create table_meta.");
        }

        table_meta->SerializeTo(table_meta_page->GetData());
        buffer_pool_manager_->UnpinPage(meta_page_id, true);

        TableInfo *new_table_info = TableInfo::Create();
        new_table_info->Init(table_meta, nullptr);

        catalog_meta_->GetTableMetaPages()->emplace(new_table_id, meta_page_id);
        table_names_[table_name] = new_table_id;
        tables_[new_table_id] = new_table_info;
        
        if (FlushCatalogMetaPage() != DB_SUCCESS) {
            throw std::runtime_error("Failed to flush catalog meta page after creating table.");
        }

        // Set output parameter and return
        table_info = new_table_info;
        LOG(INFO) << "CatalogManager: Table '" << table_name << "', id = " << new_table_id
 << " successfully created.";
        return DB_SUCCESS;
    } catch (const std::exception &e) {
        LOG(ERROR) << "CatalogManager: Creat table '" << table_name << "' failed." << e.what();

        // Rollback
        next_table_id_.fetch_sub(1);

        if (catalog_meta_ && catalog_meta_->GetTableMetaPages()->count(new_table_id)) {
            catalog_meta_->GetTableMetaPages()->erase(new_table_id);
        }
        if (table_names_.count(table_name)) {
            table_names_.erase(table_name);
        }
        if (tables_.count(new_table_id)) {
            tables_.erase(new_table_id);
        }
        if (meta_page_id != INVALID_PAGE_ID) {
            buffer_pool_manager_->DeletePage(meta_page_id);
        }
        table_info = nullptr;
        return DB_FAILED;
    }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
    auto it_name = table_names_.find(table_name);
    if (it_name == table_names_.end()) {
        return DB_TABLE_NOT_EXIST;
    }
    return GetTable(it_name->second, table_info);
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
    auto it_table = tables_.find(table_id);
    if (it_table == tables_.end()) {
        return DB_TABLE_NOT_EXIST;
    }
    table_info = it_table->second;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
    tables.clear();
    for (const auto &pair : tables_) {
        tables.push_back(pair.second);
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}