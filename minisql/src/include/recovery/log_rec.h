#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * Log Record structure
 */
struct LogRec {
    LogRec() = default;

    LogRec(LogRecType type, lsn_t lsn, lsn_t prev_lsn, txn_id_t txn_id)
        : type_(type), lsn_(lsn), prev_lsn_(prev_lsn), txn_id_(txn_id) {}

    virtual ~LogRec() = default;


    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    txn_id_t txn_id_{INVALID_TXN_ID};

    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;
};



typedef std::shared_ptr<LogRec> LogRecPtr;

struct InsertLogRec : public LogRec {
    KeyType ins_key_;
    ValType ins_val_;   
    InsertLogRec(txn_id_t txn_id, KeyType ins_key, ValType ins_val)
        : LogRec(LogRecType::kInsert, next_lsn_++, prev_lsn_map_[txn_id], txn_id),
          ins_key_(std::move(ins_key)), ins_val_(ins_val) {}


};

struct DeleteLogRec : public LogRec {
    KeyType del_key_;
    ValType del_val_{};
    DeleteLogRec(txn_id_t txn_id, KeyType del_key, ValType del_val)
        : LogRec(LogRecType::kDelete, next_lsn_++, prev_lsn_map_[txn_id], txn_id),
          del_key_(std::move(del_key)), del_val_(del_val) {}
};

struct UpdateLogRec : public LogRec {
    KeyType old_key_;
    ValType old_val_;
    KeyType new_key_;
    ValType new_val_;

    UpdateLogRec(txn_id_t txn_id, KeyType old_key, ValType old_val,
                 KeyType new_key, ValType new_val, lsn_t lsn, lsn_t prev_lsn)
        : LogRec(LogRecType::kUpdate, txn_id, lsn, prev_lsn),
          old_key_(std::move(old_key)), old_val_(old_val),
          new_key_(std::move(new_key)), new_val_(new_val) {}
};

struct BeginLogRec : public LogRec {
    BeginLogRec(txn_id_t txn_id, lsn_t lsn, lsn_t prev_lsn)
        : LogRec(LogRecType::kBegin, txn_id, lsn, prev_lsn) {}
};

struct CommitLogRec : public LogRec {
    CommitLogRec(txn_id_t txn_id, lsn_t lsn, lsn_t prev_lsn)
        : LogRec(LogRecType::kCommit, txn_id, lsn, prev_lsn) {}
};

struct AbortLogRec : public LogRec {
    AbortLogRec(txn_id_t txn_id, lsn_t lsn, lsn_t prev_lsn)
        : LogRec(LogRecType::kAbort, txn_id, lsn, prev_lsn) {}
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;


/**
 * 创建一条插入日志
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
    lsn_t current_lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = INVALID_LSN;

    if (LogRec::prev_lsn_map_.find(txn_id) != LogRec::prev_lsn_map_.end()) {
        prev_lsn = LogRec::prev_lsn_map_[txn_id];
    }
    LogRec::prev_lsn_map_[txn_id] = current_lsn;
    return std::make_shared<InsertLogRec>(txn_id, std::move(ins_key), ins_val, current_lsn, prev_lsn);
}

/**
 * 创建一条删除日志
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
    lsn_t current_lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = INVALID_LSN;

    if (LogRec::prev_lsn_map_.find(txn_id) != LogRec::prev_lsn_map_.end()) {
        prev_lsn = LogRec::prev_lsn_map_[txn_id];
    }
    LogRec::prev_lsn_map_[txn_id] = current_lsn;
    return std::make_shared<DeleteLogRec>(txn_id, std::move(del_key), del_val, current_lsn, prev_lsn);
}

/**
 * 创建一条更新日志
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
    lsn_t current_lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = INVALID_LSN;

    if (LogRec::prev_lsn_map_.find(txn_id) != LogRec::prev_lsn_map_.end()) {
        prev_lsn = LogRec::prev_lsn_map_[txn_id];
    }
    LogRec::prev_lsn_map_[txn_id] = current_lsn;
    return std::make_shared<UpdateLogRec>(txn_id, std::move(old_key), old_val, std::move(new_key), new_val, current_lsn, prev_lsn);
}

/**
 * 创建一条事务开始日志
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    lsn_t current_lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = INVALID_LSN; 

    LogRec::prev_lsn_map_[txn_id] = current_lsn;
    return std::make_shared<BeginLogRec>(txn_id, current_lsn, prev_lsn);
}

/**
 * 创建一条事务提交日志
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    lsn_t current_lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = INVALID_LSN;
    if (LogRec::prev_lsn_map_.find(txn_id) != LogRec::prev_lsn_map_.end()) {
        prev_lsn = LogRec::prev_lsn_map_[txn_id];
    }
    LogRec::prev_lsn_map_.erase(txn_id);
    return std::make_shared<CommitLogRec>(txn_id, current_lsn, prev_lsn);
}

/**
 * 创建一条事务回滚日志
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
    lsn_t current_lsn = LogRec::next_lsn_++;
    lsn_t prev_lsn = INVALID_LSN;
    if (LogRec::prev_lsn_map_.count(txn_id)) {
        prev_lsn = LogRec::prev_lsn_map_[txn_id];
    }

    // After a transaction aborts, it's no longer active.
    LogRec::prev_lsn_map_.erase(txn_id);

    return std::make_shared<AbortLogRec>(txn_id, current_lsn, prev_lsn);
}

#endif  // MINISQL_LOG_REC_H
