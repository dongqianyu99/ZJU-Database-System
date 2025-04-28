#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * Finished
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
    if (page_allocated_ >= GetMaxSupportedSize() || next_free_page_ >= GetMaxSupportedSize()) // out of space or invalid 
        return false;
    page_offset = next_free_page_;
    uint32_t byte_offset = next_free_page_ / 8;
    uint8_t bit_offset = next_free_page_ % 8;
    bytes[byte_offset] |= (1 << bit_offset); // denote page allocated
    page_allocated_ += 1;
    bool found = false;
    for (int i = 0; i < GetMaxSupportedSize() && page_allocated_ < GetMaxSupportedSize(); i++) {
        byte_offset = i / 8;
        bit_offset = i % 8;
        if (!((bytes[byte_offset] >> bit_offset) & 1)) {
            next_free_page_ = i;
            found = true;
            break;
        }
    }
    if (!found)
        next_free_page_ = GetMaxSupportedSize();
    return true;
}

/**
 * Finished
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    if (page_offset >= GetMaxSupportedSize() || IsPageFree(page_offset))  // invalid
        return false;
    uint32_t byte_offset = page_offset / 8;
    uint8_t bit_offset = page_offset % 8;
    bytes[byte_offset] &= ~(1 << bit_offset); // denote page free
    page_allocated_ -= 1;
    next_free_page_ = page_offset; // need to reuse immediately
    return true;
}

/**
 * Finished
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    uint32_t byte_offset = page_offset / 8;
    uint8_t bit_offset = page_offset % 8;
    return !(bytes[byte_offset] >> bit_offset & 1); 
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
    return !(bytes[byte_index] >> bit_index & 1); 
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;