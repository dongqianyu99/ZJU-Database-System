#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    /**
    * RecoveryManager的初始化函数
    */
    void Init(CheckPoint &last_checkpoint) {
        data_ = last_checkpoint.persist_data_; // 恢复数据到检查点时的状态
        active_txns_ = last_checkpoint.active_txns_; // 恢复活跃事务列表
        persist_lsn_ = last_checkpoint.checkpoint_lsn_; // 恢复持久化日志序列号
    }
private:
    // Rollback单个事务
    void RollbackSingleTransaction(txn_id_t txn_id_to_rollback, lsn_t last_known_lsn) {
        lsn_t current_undo_lsn = last_known_lsn;
        while (current_undo_lsn != INVALID_LSN) {
            if (log_recs_.find(current_undo_lsn) == log_recs_.end()) {
                break; 
            }
            LogRecPtr log_rec = log_recs_.at(current_undo_lsn);

            if (log_rec->txn_id_ != txn_id_to_rollback) {
                // 如果当前日志记录的事务ID与要回滚的事务ID不匹配，则跳过
                break;
            }

            if (log_rec->type_ == LogRecType::kInsert) {
                auto insert_log = std::static_pointer_cast<InsertLogRec>(log_rec);
                data_.erase(insert_log->ins_key_);
            } else if (log_rec->type_ == LogRecType::kDelete) {
                auto delete_log = std::static_pointer_cast<DeleteLogRec>(log_rec);
                data_[delete_log->del_key_] = delete_log->del_val_;
            } else if (log_rec->type_ == LogRecType::kUpdate) {
                auto update_log = std::static_pointer_cast<UpdateLogRec>(log_rec);
                // Undo: 恢复旧值,如果key改变了，新的key需要被移除，旧的key和值需要被恢复。
                if (update_log->old_key_ != update_log->new_key_) {
                    data_.erase(update_log->new_key_); 
                }
                data_[update_log->old_key_] = update_log->old_val_; 
            } else if (log_rec->type_ == LogRecType::kBegin) {
                break; // Stop when BEGIN is reached
            }
            current_undo_lsn = log_rec->prev_lsn_;
        }
    }
public:
    /**
    * 从`CheckPoint`开始，根据不同日志的类型对`KvDatabase`和活跃事务列表作出修改
    */
    void RedoPhase() {
        for (auto it = log_recs_.upper_bound(persist_lsn_); it != log_recs_.end(); ++it){
            LogRecPtr log_rec = it->second;
            txn_id_t current_txn_id = log_rec->txn_id_;
            lsn_t current_lsn = log_rec->lsn_;

            if(current_lsn <= persist_lsn_) {
                continue; // 跳过已持久化日志
            }

            if(log_rec->type_ == LogRecType::kCommit  || log_rec->type_ == LogRecType::kAbort){
                active_txns_.erase(log_rec->txn_id_); // 事务提交，移除活跃事务
            } else{
                active_txns_[log_rec -> txn_id_] = log_rec->lsn_; // 更新活跃事务列表
            }

            if(log_rec->type_ == LogRecType::kInsert) {
                auto insert_log = std::static_pointer_cast<InsertLogRec>(log_rec);
                data_[insert_log->ins_key_] = insert_log->ins_val_; // 插入操作
            } else if(log_rec->type_ == LogRecType::kDelete) {
                auto delete_log = std::static_pointer_cast<DeleteLogRec>(log_rec);
                data_.erase(delete_log->del_key_); 
            } else if(log_rec->type_ == LogRecType::kUpdate) {
                auto update_log = std::static_pointer_cast<UpdateLogRec>(log_rec);
                // Redo: 应用新的值。如果key改变了，旧的key需要被移除（如果存在）。
                if(update_log->old_key_ != update_log->new_key_) {
                    data_.erase(update_log->old_key_);
                }
                data_[update_log->new_key_] = update_log -> new_val_; // 更新操作
            } else if(log_rec->type_ == LogRecType::kAbort) {
               RollbackSingleTransaction(current_txn_id, log_rec->prev_lsn_);
            }
        }
    }
    
    /**
    * Undo阶段，对每个未完成的活跃事务进行回滚
    */
    void UndoPhase() {
        // 对Redo阶段结束后仍在active_txns_中的事务进行回滚
        ATT txns_to_undo = active_txns_;
        for (const auto& txn_entry : txns_to_undo) {
            RollbackSingleTransaction(txn_entry.first, txn_entry.second);
        }
        // 清空活跃事务列表
        active_txns_.clear();
    }

    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
