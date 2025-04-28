#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * Finished
 */
page_id_t DiskManager::AllocatePage() { // return logical page id
    DiskFileMetaPage *metaPage = reinterpret_cast<DiskFileMetaPage *>(meta_data_); // read as `DiskFileMetaPage`
    uint32_t allocatePages = metaPage->GetAllocatedPages();
    uint32_t extentNums = metaPage->GetExtentNums();
    if (allocatePages >= MAX_VALID_PAGE_ID) // no space left
        return INVALID_PAGE_ID;
    
    // find availabel extent
    uint32_t extentID;
    for (extentID = 0; extentID < extentNums; extentID++) {
        if (metaPage->GetExtentUsedPage(extentID) < BITMAP_SIZE) {
            break;
        }
    }

    // get physical page id of the extent meta page
    page_id_t physicalPageId = 1 // disk meta page
                               + (1 + BITMAP_SIZE) // extent = extent meta page + extent pages
                               * extentID;

    // modify extent meta page
    char *buffer = new char[PAGE_SIZE];
    ReadPhysicalPage(physicalPageId, buffer);
    BitmapPage<PAGE_SIZE> *extentMetaPage = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buffer);
    uint32_t page_offset = 0;
    extentMetaPage->AllocatePage(page_offset);
    WritePhysicalPage(physicalPageId, buffer);
    delete[] buffer;
    metaPage->num_allocated_pages_++;
    metaPage->extent_used_page_[extentID]++;
    if (extentID >= extentNums) // extent not enough, add a new onew
        metaPage->num_extents_++;
    
    return extentID * BITMAP_SIZE + page_offset;

}

/**
 * Finished
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
    DiskFileMetaPage *metaPage = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

    // get physical page id of the extent meta page
    uint32_t physicalPageId = 1 + logical_page_id + logical_page_id / BITMAP_SIZE - logical_page_id % BITMAP_SIZE;
    uint32_t extentID = logical_page_id / BITMAP_SIZE;

    // modify extent meta page
    char *buffer = new char[PAGE_SIZE];
    ReadPhysicalPage(physicalPageId, buffer);
    BitmapPage<PAGE_SIZE> *extentMetaPage = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buffer);
    uint32_t page_offset = logical_page_id % BITMAP_SIZE;
    extentMetaPage->DeAllocatePage(page_offset);
    WritePhysicalPage(physicalPageId, buffer);
    delete[] buffer;
    metaPage->num_allocated_pages_--;
    metaPage->extent_used_page_[extentID]--;
    // metaPage->num_extent needs no modification
}

/**
 * Finished
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
    if (logical_page_id > MAX_VALID_PAGE_ID)
        return false;

    DiskFileMetaPage *metaPage = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
    // get physical page id of the extent meta page
    uint32_t physicalPageId = 1 + logical_page_id + logical_page_id / BITMAP_SIZE - logical_page_id % BITMAP_SIZE;
    
    char *buffer = new char[PAGE_SIZE];
    ReadPhysicalPage(physicalPageId, buffer);
    BitmapPage<PAGE_SIZE> *extentMetaPage = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buffer);
    uint32_t page_offset = logical_page_id % BITMAP_SIZE;
    bool result = extentMetaPage->IsPageFree(page_offset);
    delete[] buffer;
    return result;
}

/**
 * Finished
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) { // logical page id --> physical page id
    return (logical_page_id + 1 + logical_page_id / BITMAP_SIZE);
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}