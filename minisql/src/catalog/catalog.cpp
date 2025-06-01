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
        LOG(ERROR) << "CatalogManager: CreateTable Failed - Failed to allocate page for table metadata for '" << table_name << "'.";
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
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) const {
    auto it_name = table_names_.find(table_name);
    if (it_name == table_names_.end()) {
        return DB_TABLE_NOT_EXIST;
    }
    return GetTable(it_name->second, table_info);
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) const {
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
    LOG(INFO) << "CatalogManager: Attempting to create index '" << index_name << "' on table '" << table_name << "' with type '" << index_type << "'.";
    
    // Get table
    TableInfo *table_info = nullptr;
    if (GetTable(table_name, table_info) != DB_SUCCESS || table_info == nullptr) {
        LOG(ERROR) << "CatalogManager: CreateIndex Failed - Table '" << table_name << "' not found.";
        return DB_TABLE_NOT_EXIST;
    } 

    // Check for duplication
    auto it_table_index = index_names_.find(table_name);
    if (it_table_index != index_names_.end() && it_table_index->second.count(index_name)) {
        LOG(ERROR) << "CatalogManager: CreateIndex Failed - Index '" << index_name << "' already exists.";
        return DB_INDEX_ALREADY_EXIST;
    }

    std::vector<uint32_t> key_map;
    const TableSchema *table_schema = table_info->GetSchema();
    for (const std::string &key_col_name : index_keys) {
        uint32_t col_index;
        if (table_schema->GetColumnIndex(key_col_name, col_index) != DB_SUCCESS) {
            LOG(ERROR) << "CatalogManager: CreateIndex Failed - Column '" << key_col_name << "' not found.";
        }
        key_map.push_back(col_index);
    }
    if (key_map.empty()) {
        LOG(ERROR) << "CatalogManager: CreateIndex Failed - No valid key columns provided for index '" << index_name << "'.";
        return DB_FAILED;
    }

    index_id_t new_index_id = next_index_id_.fetch_add(1);
    page_id_t meta_page_id = INVALID_PAGE_ID;
    Page *index_meta_page = buffer_pool_manager_->NewPage(meta_page_id);

    if (index_meta_page == nullptr || meta_page_id == INVALID_PAGE_ID) {
        LOG(ERROR) << "CatalogManager: CreateIndex Failed - Failed to allocate page for index metadata for '" << index_name << "'.";
        next_index_id_.fetch_sub(1); // Rollback
        return DB_FAILED;
    }

    try {
        IndexMetadata *index_meta = IndexMetadata::Create(new_index_id, index_name, table_info->GetTableId(), key_map);
        if (!index_meta) {
            throw std::runtime_error("IndexMetadata::Create failed to create index_meta.");
        }

        IndexInfo *new_index_info = IndexInfo::Create();
        new_index_info->Init(index_meta, table_info, buffer_pool_manager_);

        
        index_meta->SerializeTo(index_meta_page->GetData());
        buffer_pool_manager_->UnpinPage(meta_page_id, true); 
        
        catalog_meta_->GetIndexMetaPages()->emplace(new_index_id, meta_page_id);
        index_names_[table_name][index_name] = new_index_id;
        indexes_[new_index_id] = new_index_info;

        if (FlushCatalogMetaPage() != DB_SUCCESS) {
            throw std::runtime_error("Failed to flush catalog meta page after index creation.");
        }

        index_info = new_index_info;
        LOG(INFO) << "CatalogManager: Index '" << index_name << "', id = " << new_index_id << " created.";
        return DB_SUCCESS;
    } catch (const std::exception &e) {
        LOG(ERROR) << "CatalogManager: CreateIndex for '" <<index_name << "' failed: " << e.what();

        // Rollback
        next_index_id_.fetch_sub(1);

        if (catalog_meta_ && catalog_meta_->index_meta_pages_.count(new_index_id)) {
            catalog_meta_->index_meta_pages_.erase(new_index_id);
        }
        auto it_tn = index_names_.find(table_name);
        if (it_tn != index_names_.end()) {
            it_tn->second.erase(index_name);
            if (it_tn->second.empty()) { index_names_.erase(it_tn); }
        }
        if (indexes_.count(new_index_id)) {
            indexes_.erase(new_index_id);
        }
        if (meta_page_id != INVALID_PAGE_ID) {
            buffer_pool_manager_->DeletePage(meta_page_id);
        }
        index_info = nullptr;
        return DB_FAILED;
    } 
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
    auto it_table_map = index_names_.find(table_name);
    if (it_table_map == index_names_.end()) { return DB_TABLE_NOT_EXIST; }
    auto it_index_entry = it_table_map->second.find(index_name);
    if (it_index_entry == it_table_map->second.end()) { return DB_INDEX_NOT_FOUND; }
    
    index_id_t id = it_index_entry->second;
    auto it_index = indexes_.find(id);
    if (it_index == indexes_.end()) {
        LOG(ERROR) << "CatalogManager: GetIndex - Index ID " << id << " for '" << index_name << "' on table '" << table_name << "' not found.";
        return DB_FAILED;
    }
    index_info = it_index->second;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
    TableInfo *table_info;
    indexes.clear();
    if (GetTable(table_name, table_info) != DB_SUCCESS) { return DB_TABLE_NOT_EXIST; }

    auto it_table_entry = index_names_.find(table_name);
    if (it_table_entry != index_names_.end()) {
        const std::unordered_map<std::string, index_id_t> &table_s_indexes_map = it_table_entry->second;

        for (const auto &pair : table_s_indexes_map ) {
            // pair.first: index_name
            // pair.second: index_id
            index_id_t cur_index_id = pair.second;
            auto it_index = indexes_.find(cur_index_id);
            if (it_index == indexes_.end()) {
                indexes.push_back(it_index->second);
            } else {
                LOG(ERROR) << "CatalogManager: GetTableIndexs - Index ID " << cur_index_id << " for '" << pair.first << "' on table '" << table_name << "' not found.";
            }
        }
    }

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
    LOG(INFO) << "CatalogManager: Attempting to drop table '" << table_name << "'.";

    TableInfo *table_info;
    if (GetTable(table_name, table_info) != DB_SUCCESS) {
        return DB_TABLE_NOT_EXIST;
    }
    table_id_t table_id = table_info->GetTableId();

    // Drop all indexes associated
    std::vector<std::string> index_names_on_table;
    auto it_indexes = index_names_.find(table_name);
    if (it_indexes != index_names_.end()) {
        for (const auto &index_entry : it_indexes->second) {
            index_names_on_table.push_back(index_entry.first);
        }
    }
    for (const std::string &index_name : index_names_on_table) {
        if (DropIndex(table_name, index_name) != DB_SUCCESS) {
            LOG(ERROR) << "CatalogManager: Failed to drop index '" << index_name << "'.";
            return DB_FAILED;
        }
    }

    // Get TableMetadata page id
    if (!catalog_meta_->table_meta_pages_.count(table_id)) {
        LOG(ERROR) << "CatalogManager: DropTable - Table ID " << table_id << " not found in catalog_meta.";
        return DB_FAILED;
    }
    page_id_t table_meta_page_id = catalog_meta_->table_meta_pages_.at(table_id);

    // Remove from maps
    table_names_.erase(table_name);
    tables_.erase(table_id);

    // Update CatalogMeta
    catalog_meta_->table_meta_pages_.erase(table_id);

    // Delete the physical page for table metadata
    buffer_pool_manager_->DeletePage(table_meta_page_id);

    // Flush CatalogMetaPage
    if (FlushCatalogMetaPage() != DB_SUCCESS) {
        throw std::runtime_error("Failed to flush catalog meta page after droping table.");
    }

    LOG(INFO) << "CatalogManager: Table '" << table_name << "' dropped successfully.";
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
    LOG(INFO) << "CatalogManager: Attempting to drop index '" << index_name << "' from table '" << table_name <<"'.";

    TableInfo *table_info;
    if (GetTable(table_name, table_info) != DB_SUCCESS) {
        return DB_TABLE_NOT_EXIST;
    }

    auto it_indexs = index_names_.find(table_name);
    if (it_indexs == index_names_.end()) { return DB_TABLE_NOT_EXIST; }
    if (!it_indexs->second.count(index_name)) { return DB_INDEX_NOT_FOUND; }
    index_id_t index_id = it_indexs->second.at(index_name);

    auto it_index_info = indexes_.find(index_id);
    if (it_index_info == indexes_.end()) {
        LOG(ERROR) << "CatalogManager: DropIndex - Index ID " << index_id << " not found.";
        it_indexs->second.erase(index_name);
        if (it_indexs->second.empty()) {
            index_names_.erase(it_indexs);
            return DB_INDEX_NOT_FOUND;
        }
        IndexInfo *index_info = it_index_info->second;
    }

    // Get IndexMetadata page id
    if (!catalog_meta_->index_meta_pages_.count(index_id)) {
        LOG(ERROR) << "CatalogManager: DropIndex - Index ID " << index_id << "not in catalog_meta.";
        return DB_FAILED;
    }
    page_id_t index_meta_page_id = catalog_meta_->index_meta_pages_.at(index_id);

    // Remove from internal maps
    it_indexs->second.erase(index_name);
    if (it_indexs->second.empty()) {
        index_names_.erase(it_indexs);
    }
    indexes_.erase(index_id);

    // Update CatalogMeta
    catalog_meta_->index_meta_pages_.erase(index_id);

    // Delete the physical page for index metadate
    buffer_pool_manager_->DeletePage(index_meta_page_id);

    // Flush
    if (FlushCatalogMetaPage() != DB_SUCCESS) {
        throw std::runtime_error("Failed to flush catalog meta page after droping index.");
    }

    LOG(ERROR) << "CatalogManager: Index '" << index_name << "' dropped successfully.";
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
    if (catalog_meta_ == nullptr) { 
        LOG(ERROR) << "CatalogManager: FlushCatalogMetaPage failed - calog_meta_ is null.";
        return DB_FAILED;
    }
    Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    if (meta_page == nullptr) {
        LOG(ERROR) << "Failed to fetch catalog meta page for flushing.";
        return DB_FAILED;
    }

    catalog_meta_->SerializeTo(meta_page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

    LOG(INFO) << "CatalogManager: Catalog meta page flushed.";
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
    LOG(INFO) << "CatalogManager: Loading table_id " << table_id << " from " << page_id;
    Page *table_meta_page = buffer_pool_manager_->FetchPage(page_id);
    if (table_meta_page == nullptr) { return DB_FAILED; }

    TableMetadata *table_meta = nullptr;
    TableInfo *table_info = nullptr;
    try {
        TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta);
        buffer_pool_manager_->UnpinPage(page_id, true);
        if (table_meta == nullptr) { return DB_FAILED; }
        if (table_meta->GetTableId() != table_id) {
            LOG(ERROR) << "Table ID mismatch on load.";
            return DB_FAILED;
        } 

        table_info = TableInfo::Create();
        table_info->Init(table_meta, nullptr);
        table_meta = nullptr;

        table_names_[table_info->GetTableName()] = table_id;
        tables_[table_id] = table_info;

        LOG(ERROR) << "CatalogManager: Loaded table '" << table_info->GetTableName() << "'.";
        return DB_SUCCESS;
    } catch (const std::exception &e) {
        LOG(ERROR) << "Exception during table loading: " << e.what();
        if (table_meta_page != nullptr) {
            buffer_pool_manager_->UnpinPage(page_id, false);
            return DB_FAILED;
        }
    }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
    LOG(INFO) << "CatalogManager: Loading index_id " << index_id << " from " << page_id;
    Page *index_meta_page = buffer_pool_manager_->FetchPage(page_id);
    if (index_meta_page == nullptr) { return DB_FAILED; }

    IndexMetadata *index_meta = nullptr;
    IndexInfo *index_info = nullptr;
    TableInfo *table_info = nullptr;
    try {
        IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta);
        buffer_pool_manager_->UnpinPage(page_id, false);
        if (index_meta == nullptr) { return DB_FAILED; }
        if (index_meta->GetIndexId() != index_id) {
            LOG(ERROR) << "Index ID mismatch on load.";
            return DB_FAILED;
        }
        
        if (GetTable(index_meta->GetTableId(), table_info) != DB_SUCCESS) {
            LOG(ERROR) << "Table_id " << index_meta->GetTableId() << " for index " << index_id << " not found.";
            return DB_TABLE_NOT_EXIST;
        }

        index_info = IndexInfo::Create();
        index_info->Init(index_meta, table_info, buffer_pool_manager_);
        
        indexes_[index_id] = index_info;
        index_names_[table_info->GetTableName()][index_info->GetIndexName()] = index_id;
        
        LOG(INFO) << "CatalogManager: Loaded index '" << index_info->GetIndexName() << "'.";
        return DB_SUCCESS;
    } catch (const std::exception &e) {
        LOG(ERROR) << "Exception during index loading: " << e.what();
        if (index_meta_page != nullptr) {
            buffer_pool_manager_->UnpinPage(page_id, false);
            return DB_FAILED;
        }
    }
}