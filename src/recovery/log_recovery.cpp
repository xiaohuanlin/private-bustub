//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_recovery.cpp
//
// Identification: src/recovery/log_recovery.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_recovery.h"

#include "storage/page/table_page.h"

namespace bustub {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data, LogRecord *log_record) {
    if (buffer_offset_ + LogRecord::HEADER_SIZE > LOG_BUFFER_SIZE) {
        return false;
    }
    
    // read record
    int32_t size = *static_cast<int32_t*>((void *)(data + buffer_offset_));
    lsn_t lsn = *static_cast<lsn_t*>((void *)(data + buffer_offset_ + 4));
    txn_id_t txn_id = *static_cast<txn_id_t*>((void *)(data + buffer_offset_ + 8));
    lsn_t prev_lsn = *static_cast<lsn_t*>((void *)(data + buffer_offset_ + 12));
    LogRecordType record_type = *static_cast<LogRecordType*>((void *)(data + buffer_offset_ + 16));

    if (record_type != LogRecordType::BEGIN && record_type != LogRecordType::ABORT && record_type != LogRecordType::COMMIT
        && record_type != LogRecordType::INSERT && record_type != LogRecordType::MARKDELETE && record_type != LogRecordType::APPLYDELETE
        && record_type != LogRecordType::ROLLBACKDELETE && record_type != LogRecordType::UPDATE && record_type != LogRecordType::NEWPAGE) {
        return false;
    }
    if (buffer_offset_ + size > LOG_BUFFER_SIZE) {
        return false;
    }

    const char *record_ptr = data + buffer_offset_ + LogRecord::HEADER_SIZE;
    switch (record_type) {
        case LogRecordType::BEGIN:
        case LogRecordType::ABORT:
        case LogRecordType::COMMIT: {
            *log_record = LogRecord(txn_id, prev_lsn, record_type);
            break;
        }
        case LogRecordType::INSERT:
        case LogRecordType::MARKDELETE:
        case LogRecordType::APPLYDELETE:
        case LogRecordType::ROLLBACKDELETE: {
            RID tuple_id = *static_cast<RID*>((void *)record_ptr);
            Tuple *tuple = new Tuple();
            tuple->DeserializeFrom(record_ptr + sizeof(RID));
            *log_record = LogRecord(txn_id, prev_lsn, record_type, tuple_id, *tuple);
            break;
        }
        case LogRecordType::UPDATE: {
            RID tuple_id = *static_cast<RID*>((void *)record_ptr);
            Tuple *old_tuple = new Tuple();
            old_tuple->DeserializeFrom(record_ptr + sizeof(RID));

            Tuple *new_tuple = new Tuple();
            new_tuple->DeserializeFrom(record_ptr + sizeof(RID) + sizeof(uint32_t) + old_tuple->GetLength());
            *log_record = LogRecord(txn_id, prev_lsn, record_type, tuple_id, *old_tuple, *new_tuple);
            break;
        }
        case LogRecordType::NEWPAGE: {
            page_id_t prev_page_id = *static_cast<page_id_t*>((void *)record_ptr);
            page_id_t page_id = *static_cast<page_id_t*>((void *)(record_ptr + sizeof(page_id_t)));
            *log_record = LogRecord(txn_id, prev_lsn, record_type, prev_page_id, page_id);
            break;
        }
        default:
            break;
    }
    log_record->lsn_ = lsn;
    return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
    while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_)) {
        LogRecord log_record;
        while (DeserializeLogRecord(log_buffer_, &log_record)) {
            txn_id_t txn_id = log_record.GetTxnId();
            std::cout << "redo: " << log_record.ToString() << std::endl;

            switch (log_record.GetLogRecordType()) {
                case LogRecordType::BEGIN:
                    active_txn_[txn_id] = log_record.GetLSN();
                    break;
                case LogRecordType::ABORT:
                case LogRecordType::COMMIT: {
                    active_txn_.erase(txn_id);
                    break;
                }
                case LogRecordType::INSERT: {
                    RID rid = log_record.GetInsertRID();
                    TablePage* table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                    if (table_page->GetLSN() < log_record.GetLSN()) {
                        table_page->InsertTuple(log_record.insert_tuple_, &rid, nullptr, nullptr, nullptr);
                    }
                    active_txn_[txn_id] = std::max(table_page->GetLSN(), log_record.GetLSN());
                    break;
                }
                case LogRecordType::MARKDELETE: {
                    RID rid = log_record.GetDeleteRID();
                    TablePage* table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                    if (table_page->GetLSN() < log_record.GetLSN()) {
                        table_page->MarkDelete(rid, nullptr, nullptr, nullptr);
                    }
                    active_txn_[txn_id] = std::max(table_page->GetLSN(), log_record.GetLSN());
                    break;
                }
                case LogRecordType::APPLYDELETE: {
                    RID rid = log_record.GetDeleteRID();
                    TablePage* table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                    if (table_page->GetLSN() < log_record.GetLSN()) {
                        table_page->ApplyDelete(rid, nullptr, nullptr);
                    }
                    active_txn_[txn_id] = std::max(table_page->GetLSN(), log_record.GetLSN());
                    break;
                }
                case LogRecordType::ROLLBACKDELETE: {
                    RID rid = log_record.GetDeleteRID();
                    TablePage* table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                    if (table_page->GetLSN() < log_record.GetLSN()) {
                        table_page->RollbackDelete(rid, nullptr, nullptr);
                    }
                    active_txn_[txn_id] = std::max(table_page->GetLSN(), log_record.GetLSN());
                    break;
                }
                case LogRecordType::UPDATE: {
                    RID rid = log_record.update_rid_;
                    TablePage* table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                    if (table_page->GetLSN() < log_record.GetLSN()) {
                        table_page->UpdateTuple(log_record.new_tuple_, &log_record.old_tuple_, rid, nullptr, nullptr, nullptr);
                    }
                    active_txn_[txn_id] = std::max(table_page->GetLSN(), log_record.GetLSN());
                    break;
                }
                case LogRecordType::NEWPAGE: {
                    page_id_t prev_page_id = log_record.prev_page_id_;
                    page_id_t page_id = log_record.page_id_;
                    TablePage* table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
                    if (table_page->GetLSN() < log_record.GetLSN()) {
                        table_page->Init(page_id, PAGE_SIZE, prev_page_id, nullptr, nullptr);
                    }
                    active_txn_[txn_id] = std::max(table_page->GetLSN(), log_record.GetLSN());
                    break;
                }
                default:
                    break;
            }

            lsn_mapping_[log_record.GetLSN()] = buffer_offset_;
            buffer_offset_ += log_record.size_;
        }
        offset_ = buffer_offset_;
        buffer_offset_ = 0;
    }
    buffer_pool_manager_->FlushAllPages();
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
    for (auto &pair: active_txn_) {
        // undo every transaction
        lsn_t lsn = pair.second;
        offset_ = lsn_mapping_[lsn];

        LogRecord log_record;
        buffer_offset_ = 0;
        while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_)) {
            if (!DeserializeLogRecord(log_buffer_, &log_record)) {
                // we can't undo more log
                break;
            }
            std::cout << "undo: " << log_record.ToString() << std::endl;

            switch (log_record.GetLogRecordType()) {
                case LogRecordType::BEGIN:
                case LogRecordType::ABORT:
                case LogRecordType::COMMIT: {
                    break;
                }
                case LogRecordType::INSERT: {
                    RID rid = log_record.GetInsertRID();
                    TablePage* table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                    table_page->ApplyDelete(rid, nullptr, nullptr);
                    break;
                }
                case LogRecordType::MARKDELETE: {
                    RID rid = log_record.GetDeleteRID();
                    TablePage* table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                    table_page->RollbackDelete(rid, nullptr, nullptr);
                    break;
                }
                case LogRecordType::APPLYDELETE: {
                    RID rid = log_record.GetDeleteRID();
                    TablePage* table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                    table_page->InsertTuple(log_record.delete_tuple_, &rid, nullptr, nullptr, nullptr);
                    break;
                }
                case LogRecordType::ROLLBACKDELETE: {
                    RID rid = log_record.GetDeleteRID();
                    TablePage* table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                    table_page->MarkDelete(rid, nullptr, nullptr, nullptr);
                    break;
                }
                case LogRecordType::UPDATE: {
                    RID rid = log_record.update_rid_;
                    TablePage* table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
                    table_page->UpdateTuple(log_record.new_tuple_, &log_record.old_tuple_, rid, nullptr, nullptr, nullptr);
                    break;
                }
                case LogRecordType::NEWPAGE:
                default:
                    break;
            }

            lsn_t prev_lsn = log_record.GetPrevLSN();
            if (prev_lsn == INVALID_LSN) {
                break;
            }
            offset_ = lsn_mapping_[prev_lsn];
            buffer_offset_ = 0;
        }
    }
}

}  // namespace bustub
