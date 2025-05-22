#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

    char *buffer_pos = buf;

    /**
     * ------------------------------------------------------
     * | Field Nums | Null Bitmap | Field-1 | ... | Field-N |
     * ------------------------------------------------------
     */

    // Write Field Nums.
    uint32_t field_nums = schema->GetColumnCount();
    MACH_WRITE_UINT32(buffer_pos, field_nums);
    buffer_pos += sizeof(field_nums);

    // Write Null Bitmap and Fields.
    uint32_t bitmap_len = (field_nums + 7) / 8; // Bytes to hold the Null Bitmap.
    std::memset(buffer_pos, 0, bitmap_len); // Set 0 at the beginning.
    char *nullBitMap_pos = buffer_pos;
    buffer_pos += bitmap_len;
    for (uint32_t i = 0; i < field_nums; i++) {
        if (fields_[i]->IsNull()) { // Set 1 if is NULL.
            nullBitMap_pos[i / 8] |= static_cast<char>(1 << (i % 8)); // i/8: byte-offset, i%8: bit-offset
        }
        else {
            buffer_pos += fields_[i]->SerializeTo(buffer_pos);
        }
    }

    return static_cast<uint32_t>(buffer_pos - buf);
}

/**
 * TODO: Student Implement
 */
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(fields_.empty(), "Non empty field in row.");

    char *buffer_pos = buf;

    // Read Field Nums and check.
    uint32_t field_nums = MACH_READ_UINT32(buffer_pos);
    // ASSERT(schema->GetColumnCount() == field_nums, "Fields size do not match schema's column size.");
    buffer_pos += sizeof(field_nums);

    // Read Null Bitmap and Fields.
    uint32_t bitmap_len = (field_nums + 7) / 8; // Bytes to hold the Null Bitmap.
    char *bitmap = buffer_pos;
    buffer_pos += bitmap_len;
    // fields_.clear();
    for (uint32_t i = 0; i < field_nums; i++) {
        Field *field_tmp = nullptr;
        if (bitmap[i / 8] &static_cast<char>(1 << (i % 8)))
            buffer_pos += Field::DeserializeFrom(buffer_pos, schema->GetColumn(i)->GetType(), &field_tmp, true);
        else 
            buffer_pos += Field::DeserializeFrom(buffer_pos, schema->GetColumn(i)->GetType(), &field_tmp, false);
        fields_.push_back(field_tmp);
    }

    return static_cast<uint32_t>(buffer_pos - buf);
}

/**
 * TODO: Student Implement
 */
uint32_t Row::GetSerializedSize(Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

    uint32_t size = 0;
    size += sizeof(uint32_t);
    size += (fields_.size() + 7) / 8;
    for (uint32_t i = 0; i < fields_.size(); i++) {
        if (!fields_[i]->IsNull())
            size += fields_[i]->GetSerializedSize();
    }

    return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
    auto columns = key_schema->GetColumns();
    std::vector<Field> fields;
    uint32_t idx;
    for (auto column : columns) {
      schema->GetColumnIndex(column->GetName(), idx);
      fields.emplace_back(*this->GetField(idx));
    }
    key_row = Row(fields);
}
