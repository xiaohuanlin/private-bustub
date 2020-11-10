#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "storage/index/index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * Typedefs
 */
using table_oid_t = uint32_t;
using column_oid_t = uint32_t;

/**
 * Metadata about a table.
 */
struct TableMetadata {
  TableMetadata(Schema schema, std::string name, std::unique_ptr<TableHeap> &&table, table_oid_t oid)
      : schema_(std::move(schema)), name_(std::move(name)), table_(std::move(table)), oid_(oid) {}
  Schema schema_;
  std::string name_;
  std::unique_ptr<TableHeap> table_;
  table_oid_t oid_;
};

/**
 * SimpleCatalog is a non-persistent catalog that is designed for the executor to use.
 * It handles table creation and table lookup.
 */
class SimpleCatalog {
 public:
  /**
   * Creates a new catalog object.
   * @param bpm the buffer pool manager backing tables created by this catalog
   * @param lock_manager the lock manager in use by the system
   * @param log_manager the log manager in use by the system
   */
  SimpleCatalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager *log_manager)
      : bpm_{bpm}, lock_manager_{lock_manager}, log_manager_{log_manager} {}

  /**
   * Create a new table and return its metadata.
   * @param txn the transaction in which the table is being created
   * @param table_name the name of the new table
   * @param schema the schema of the new table
   * @return a pointer to the metadata of the new table
   */
  TableMetadata *CreateTable(Transaction *txn, const std::string &table_name, const Schema &schema) {
    BUSTUB_ASSERT(names_.count(table_name) == 0, "Table names should be unique!");
    catalog_latch_.WLock();
    table_oid_t table_oid = next_table_oid_++;
    names_[table_name] = table_oid;
    tables_[table_oid] = std::unique_ptr<TableMetadata>(new TableMetadata(schema, table_name, nullptr, table_oid));
    catalog_latch_.WUnlock();
    return tables_[table_oid].get();
  }

  /** @return table metadata by name */
  TableMetadata *GetTable(const std::string &table_name) {
    catalog_latch_.RLock();
    auto tid_iter = names_.find(table_name);
    if (tid_iter == names_.end()) {
      catalog_latch_.RUnlock();
      throw std::out_of_range("table name not exist");
    }
    catalog_latch_.RUnlock();
    return GetTable(tid_iter->second);
  }

  /** @return table metadata by oid */
  TableMetadata *GetTable(table_oid_t table_oid) {
    catalog_latch_.RLock();
    auto table_iter = tables_.find(table_oid);
    catalog_latch_.RUnlock();
    return table_iter == tables_.end() ? nullptr: (table_iter->second).get();
  }

 private:
  [[maybe_unused]] BufferPoolManager *bpm_;
  [[maybe_unused]] LockManager *lock_manager_;
  [[maybe_unused]] LogManager *log_manager_;

  /** tables_ : table identifiers -> table metadata. Note that tables_ owns all table metadata. */
  std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>> tables_;
  /** names_ : table names -> table identifiers */
  std::unordered_map<std::string, table_oid_t> names_;
  /** The next table identifier to be used. */
  std::atomic<table_oid_t> next_table_oid_{0};

  ReaderWriterLatch catalog_latch_;
};
}  // namespace bustub
