#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
    /**
     * -------------------------------------------------------------------------------------------------------------
     * | COLUMN_MAGIC_NUM | name_len | name_ | type_ | len_{0} | table_ind_{0} | nullable_{false} | unique_{false} |
     * -------------------------------------------------------------------------------------------------------------
     */
  
    char *buffer_pos = buf;

    // Write COLUMN_MAGIC_NUM
    MACH_WRITE_UINT32(buffer_pos, Column::COLUMN_MAGIC_NUM);
    buffer_pos += sizeof(uint32_t);

    // Write name_len & name_
    MACH_WRITE_UINT32(buffer_pos, static_cast<uint32_t>(name_.size()));
    buffer_pos += sizeof(uint32_t);
    MACH_WRITE_STRING(buffer_pos, name_);
    buffer_pos += name_.size();

    // Write type_
    MACH_WRITE_UINT32(buffer_pos, static_cast<uint32_t>(type_));
    buffer_pos += sizeof(uint32_t);

    // Write len_
    MACH_WRITE_UINT32(buffer_pos, len_);
    buffer_pos += sizeof(uint32_t);

    // Write table_ind_
    MACH_WRITE_UINT32(buffer_pos, table_ind_);
    buffer_pos += sizeof(uint32_t);

    // Write nullable_ & unique_{false}
    *buffer_pos = static_cast<char>(nullable_);
    buffer_pos += sizeof(char);
    *buffer_pos = static_cast<char>(unique_);
    buffer_pos += sizeof(char);

    return static_cast<uint32_t>(buffer_pos - buf);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
    return sizeof(uint32_t) // COLUMN_MAGIC_NUM: 4 Bytes
           + sizeof(uint32_t) // name_len: 4 Bytes
           + name_.size() // name_
           + sizeof(uint32_t) // type_: 4 Bytes
           + sizeof(uint32_t) // len_: 4 Bytes
           + sizeof(uint32_t) // table_ind_: 4 Bytes
           + sizeof(char) // nullable_: 1 Byte
           + sizeof(char); // unique_: 1 Byte
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
    char *buffer_pos = buf;

    // Read COLUMN_MAGIC_NUM
    uint32_t magicNum = MACH_READ_UINT32(buffer_pos);
    ASSERT(magicNum == Column::COLUMN_MAGIC_NUM, "COLUMN_MAGIC_NUM doesn't mach.");
    buffer_pos += sizeof(uint32_t);

    // Read name_
    uint32_t name_len = MACH_READ_UINT32(buffer_pos);
    buffer_pos += sizeof(uint32_t);
    std::string name(buffer_pos, name_len);
    buffer_pos += name_len;

    // Read type_
    TypeId type = static_cast<TypeId>(MACH_READ_UINT32(buffer_pos));
    buffer_pos += sizeof(uint32_t);

    // Read len_
    uint32_t len = MACH_READ_UINT32(buffer_pos);
    buffer_pos += sizeof(uint32_t);

    // Read table_ind_
    uint32_t table_ind = MACH_READ_UINT32(buffer_pos);
    buffer_pos += sizeof(uint32_t);

    // Read nullable_ & unique_
    bool nullable = static_cast<bool>(*buffer_pos);
    buffer_pos += sizeof(char);
    bool unique = static_cast<bool>(*buffer_pos);
    buffer_pos += sizeof(char);

    /**
     * For char type, len_ is the maximum byte length of the string data, otherwise is the fixed size
     */
    if (type == TypeId::kTypeChar)
        column = new Column(name, type, len, table_ind, nullable, unique);
    else 
        column = new Column(name, type, table_ind, nullable, unique);

    return static_cast<uint32_t>(buffer_pos - buf);
}
