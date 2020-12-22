//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"

namespace bustub {

std::future<void> LogManager::SyncFlush(bool wait_until_flush, Page *flush_page) {
    if (promise_) {
        while (!future_done_) {
            continue;
        }
        delete promise_;
        promise_ = nullptr;
    }
    future_done_ = false;
    swap_done_ = false;
    promise_ = new std::promise<void>();

    // wait for swap notify
    std::unique_lock<std::mutex> lk(latch_);
    std::cout << "wait for swap notify" << std::endl;
    cv_.wait(lk, [&]() {return swap_done_ == true;});
    std::cout << "wait swap done" << std::endl;

    std::future<void> future = promise_->get_future();

    if (wait_until_flush) {
        std::cout << "wait for future notify" << std::endl;
        future.wait();
        std::cout << "wait future done" << std::endl;
    }
    return future;
}

/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when timeout or the log buffer is full or buffer
 * pool manager wants to force flush (it only happens when the flushed page has
 * a larger LSN than persistent LSN)
 *
 * This thread runs forever until system shutdown/StopFlushThread
 */
void LogManager::RunFlushThread() {
    enable_logging = true;

    flush_thread_ = new std::thread([&] () {
        while (thread_run_forever_) {
            if (promise_ == nullptr || future_done_ == true) {
                continue;
            }

            // swap log buffer with flush buffer
            char *tmp;
            lsn_t tmp_lsn;

            {
                std::lock_guard<std::mutex> lk(latch_);
                tmp = log_buffer_;
                log_buffer_ = flush_buffer_;
                flush_buffer_ = tmp;
                tmp_lsn = next_lsn_ - 1;
                swap_done_ = true;
            }
            // swap done, notify other thread
            cv_.notify_one();
            std::cout << "thread swap done" << std::endl;

            // flush log data to disk_manager
            disk_manager_->WriteLog(flush_buffer_, offset_);
            SetPersistentLSN(tmp_lsn);
            offset_ = 0;
            promise_->set_value();
            future_done_ = true;
            std::cout << "thread flush done" << std::endl;
        }
    });
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
    thread_run_forever_ = false;
    enable_logging = false;
    flush_thread_->join();
    delete flush_thread_;
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
    if (offset_ + log_record->size_ > LOG_BUFFER_SIZE) {
        // buffer is not enough
        SyncFlush();
    }

    std::lock_guard<std::mutex> lg(latch_);
    log_record->lsn_ = next_lsn_++;
    memcpy(log_buffer_ + offset_, &log_record->size_, 4);
    memcpy(log_buffer_ + offset_ + 4, &log_record->lsn_, 4);
    memcpy(log_buffer_ + offset_ + 8, &log_record->txn_id_, 4);
    memcpy(log_buffer_ + offset_ + 12, &log_record->prev_lsn_, 4);
    memcpy(log_buffer_ + offset_ + 16, &log_record->log_record_type_, 4);
    int pos = offset_ + 20;
    
    if (log_record->log_record_type_ == LogRecordType::INSERT) {
        memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(RID));
        pos += sizeof(RID);
        log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
    } else if (log_record->log_record_type_ == LogRecordType::MARKDELETE ||
                log_record->log_record_type_ == LogRecordType::APPLYDELETE ||
                log_record->log_record_type_ == LogRecordType::ROLLBACKDELETE) {
        memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(RID));
        pos += sizeof(RID);
        log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
    } else if (log_record->log_record_type_ == LogRecordType::UPDATE) {
        memcpy(log_buffer_ + pos, &log_record->update_rid_, sizeof(RID));
        pos += sizeof(RID);
        log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
        // add old tuple size
        pos += (sizeof(int32_t) + log_record->old_tuple_.GetLength());
        log_record->new_tuple_.SerializeTo(log_buffer_ + pos);
    } else if (log_record->log_record_type_ == LogRecordType::NEWPAGE) {
        memcpy(log_buffer_ + pos, &log_record->prev_page_id_, sizeof(page_id_t));
        pos += sizeof(page_id_t);
        memcpy(log_buffer_ + pos, &log_record->page_id_, sizeof(page_id_t));
    }

    offset_ += log_record->size_;
    return log_record->lsn_;
}

}  // namespace bustub
