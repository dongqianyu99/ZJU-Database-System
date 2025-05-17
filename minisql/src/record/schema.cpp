#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
    /**
     * ----------------------------------------------------------
     * | SCHEMA_MAGIC_NUM | columns_len | columns_ | is_manage_ |
     * ----------------------------------------------------------
     */

    char *buffer_pos = buf;

    // Write SCHEMA_MAGIC_NUM
    MACH_WRITE_UINT32(buffer_pos, Schema::SCHEMA_MAGIC_NUM);
    buffer_pos += sizeof(uint32_t);

    // Write columns_len & columns_
    MACH_WRITE_UINT32(buffer_pos, static_cast<uint32_t>(columns_.size()));
    buffer_pos += sizeof(uint32_t);
    for (const auto *it : columns_) 
        buffer_pos += it->SerializeTo(buffer_pos);
    
    // Write is_manage_
    *buffer_pos = static_cast<char>(is_manage_);
    buffer_pos += sizeof(char);

    return static_cast<uint32_t>(buffer_pos - buf);
}

/**
 * TODO: Student Implement
 */
uint32_t Schema::GetSerializedSize() const {
    uint32_t size = sizeof(uint32_t) // SCHEMA_MAGIC_NUM: 4 Byte
                    + sizeof(uint32_t); // columns_len: 4 Byte
    for (const auto *it : columns_)
        size += it->GetSerializedSize();
    size += sizeof(char); // is_manage_: 1 Byte
    return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
    char *buffer_pos = buf;

    // Read SCHEMA_MAGIC_NUM
    uint32_t magicNum = MACH_READ_UINT32(buffer_pos);
    ASSERT(magicNum == Schema::SCHEMA_MAGIC_NUM, "SCHEMA_MAGIC_NUM doesn't mach.");
    buffer_pos += sizeof(uint32_t);

    // Read columns_
    uint32_t columns_len = MACH_READ_UINT32(buffer_pos);
    buffer_pos += sizeof(uint32_t);
    std::vector<Column *> columns;
    for (uint32_t i = 0; i < columns_len; i++) {
        Column *column_tmp = nullptr;
        buffer_pos += Column::DeserializeFrom(buffer_pos, column_tmp);
        columns.push_back(column_tmp);
    }

    // Read is_manage_
    bool is_manage = static_cast<bool>(*buffer_pos);
    buffer_pos += sizeof(bool);

    // Construct a new schema.
    schema = new Schema(columns, is_manage);

    return static_cast<uint32_t>(buffer_pos - buf);
}