//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// recovery_test.cpp
//
// Identification: test/execution/recovery_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <vector>

#include "common/bustub_instance.h"
#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"
#include "logging/common.h"
#include "recovery/log_recovery.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(RecoveryTest, RedoTest) {
  remove("test.db");
  remove("test.log");

  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  RID rid;
  RID rid1;
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);

  auto val_1 = tuple.GetValue(&schema, 1);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val1_1 = tuple1.GetValue(&schema, 1);
  auto val1_0 = tuple1.GetValue(&schema, 0);

  ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));
  ASSERT_TRUE(test_table->InsertTuple(tuple1, &rid1, txn));

  bustub_instance->transaction_manager_->Commit(txn);
  LOG_INFO("Commit txn");

  delete txn;
  delete test_table;

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("System restart...");
  bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Check if tuple is not in table before recovery");
  Tuple old_tuple;
  Tuple old_tuple1;
  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);
  ASSERT_FALSE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_FALSE(test_table->GetTuple(rid1, &old_tuple1, txn));
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;

  LOG_INFO("Begin recovery");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  LOG_INFO("Redo underway...");
  log_recovery->Redo();
  LOG_INFO("Undo underway...");
  log_recovery->Undo();

  LOG_INFO("Check if recovery success");
  txn = bustub_instance->transaction_manager_->Begin();
  delete test_table;
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_TRUE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_TRUE(test_table->GetTuple(rid1, &old_tuple1, txn));
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;
  delete test_table;
  delete log_recovery;

  ASSERT_EQ(old_tuple.GetValue(&schema, 1).CompareEquals(val_1), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple1.GetValue(&schema, 1).CompareEquals(val1_1), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple1.GetValue(&schema, 0).CompareEquals(val1_0), CmpBool::CmpTrue);

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, UndoTest) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  RID rid;
  const Tuple tuple = ConstructTuple(&schema);

  auto val_0 = tuple.GetValue(&schema, 0);
  auto val_1 = tuple.GetValue(&schema, 1);

  ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));

  LOG_INFO("Table page content is written to disk");
  bustub_instance->buffer_pool_manager_->FlushPage(first_page_id);

  delete txn;
  delete test_table;

  LOG_INFO("System crash before commit");
  delete bustub_instance;

  LOG_INFO("System restarted..");
  bustub_instance = new BustubInstance("test.db");

  LOG_INFO("Check if tuple exists before recovery");
  Tuple old_tuple;
  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_TRUE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple.GetValue(&schema, 1).CompareEquals(val_1), CmpBool::CmpTrue);
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;

  LOG_INFO("Recovery started..");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  LOG_INFO("Check if failed txn is undo successfully");
  txn = bustub_instance->transaction_manager_->Begin();
  delete test_table;
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_FALSE(test_table->GetTuple(rid, &old_tuple, txn));
  bustub_instance->transaction_manager_->Commit(txn);

  delete txn;
  delete test_table;
  delete log_recovery;

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

TEST(RecoveryTest, MultiTransactionTest) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn1 = bustub_instance->transaction_manager_->Begin();
  Transaction *txn2 = bustub_instance->transaction_manager_->Begin();
  Transaction *txn3 = bustub_instance->transaction_manager_->Begin();
  auto *test_table1 = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn1);
  page_id_t first_page_id1 = test_table1->GetFirstPageId();
  auto *test_table2 = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn2);
  page_id_t first_page_id2 = test_table2->GetFirstPageId();
  auto *test_table3 = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn3);
  page_id_t first_page_id3 = test_table3->GetFirstPageId();

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};
  RID rid1, rid2, rid3;
  const Tuple tuple1 = ConstructTuple(&schema);
  const Tuple tuple2 = ConstructTuple(&schema);
  const Tuple tuple3 = ConstructTuple(&schema);

  auto val1_0 = tuple1.GetValue(&schema, 0);
  auto val1_1 = tuple1.GetValue(&schema, 1);
  auto val2_0 = tuple2.GetValue(&schema, 0);
  auto val2_1 = tuple2.GetValue(&schema, 1);
  auto val3_0 = tuple3.GetValue(&schema, 0);
  auto val3_1 = tuple3.GetValue(&schema, 1);

  // t1, t2, t3: insert one row
  ASSERT_TRUE(test_table1->InsertTuple(tuple1, &rid1, txn1));
  ASSERT_TRUE(test_table2->InsertTuple(tuple2, &rid2, txn2));
  ASSERT_TRUE(test_table3->InsertTuple(tuple3, &rid3, txn3));

  LOG_INFO("Table page content is written to disk");
  bustub_instance->buffer_pool_manager_->FlushAllPages();

  // t2 update
  const Tuple new_tuple2 = ConstructTuple(&schema);
  ASSERT_TRUE(test_table2->UpdateTuple(new_tuple2, rid2, txn2));
  auto new_val2_0 = new_tuple2.GetValue(&schema, 0);
  auto new_val2_1 = new_tuple2.GetValue(&schema, 1);

  // t3 delete
  ASSERT_TRUE(test_table3->MarkDelete(rid3, txn3));

  // t2, t3 commit
  bustub_instance->transaction_manager_->Commit(txn2);
  bustub_instance->transaction_manager_->Commit(txn3);

  delete txn1;
  delete txn2;
  delete txn3;
  delete test_table1;
  delete test_table2;
  delete test_table3;

  LOG_INFO("System crash before commit");
  delete bustub_instance;

  LOG_INFO("System restarted..");
  bustub_instance = new BustubInstance("test.db");

  LOG_INFO("Recovery started..");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  LOG_INFO("Check if txns are all right");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  test_table1 = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, first_page_id1);
  test_table2 = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, first_page_id2);
  test_table3 = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, first_page_id3);
  Tuple tmp_tuple;
  // t1 no inserted row
  ASSERT_FALSE(test_table1->GetTuple(rid1, &tmp_tuple, txn));
  // t2 has a row with new data
  ASSERT_TRUE(test_table2->GetTuple(rid2, &tmp_tuple, txn));
  ASSERT_EQ(tmp_tuple.GetValue(&schema, 1).CompareEquals(new_val2_1), CmpBool::CmpTrue);
  ASSERT_EQ(tmp_tuple.GetValue(&schema, 0).CompareEquals(new_val2_0), CmpBool::CmpTrue);
  // t3 has a row with first commit
  ASSERT_FALSE(test_table3->GetTuple(rid3, &tmp_tuple, txn));
  bustub_instance->transaction_manager_->Commit(txn);

  delete txn;
  delete test_table1;
  delete test_table2;
  delete test_table3;
  delete log_recovery;

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, CheckpointTest) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  EXPECT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  EXPECT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  bustub_instance->transaction_manager_->Commit(txn);

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  std::vector<Column> cols{col1, col2};
  Schema schema{cols};

  Tuple tuple = ConstructTuple(&schema);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val_1 = tuple.GetValue(&schema, 1);

  // set log time out very high so that flush doesn't happen before checkpoint is performed
  log_timeout = std::chrono::seconds(15);

  // insert a ton of tuples
  Transaction *txn1 = bustub_instance->transaction_manager_->Begin();
  for (int i = 0; i < 1000; i++) {
    RID rid;
    EXPECT_TRUE(test_table->InsertTuple(tuple, &rid, txn1));
  }
  bustub_instance->transaction_manager_->Commit(txn1);

  // Do checkpoint
  bustub_instance->checkpoint_manager_->BeginCheckpoint();
  bustub_instance->checkpoint_manager_->EndCheckpoint();

  Page *pages = bustub_instance->buffer_pool_manager_->GetPages();
  size_t pool_size = bustub_instance->buffer_pool_manager_->GetPoolSize();

  // make sure that all pages in the buffer pool are marked as non-dirty
  bool all_pages_clean = true;
  for (size_t i = 0; i < pool_size; i++) {
    Page *page = &pages[i];
    page_id_t page_id = page->GetPageId();

    if (page_id != INVALID_PAGE_ID && page->IsDirty()) {
      all_pages_clean = false;
      break;
    }
  }
  EXPECT_TRUE(all_pages_clean);

  // compare each page in the buffer pool to that page's
  // data on disk. ensure they match after the checkpoint
  bool all_pages_match = true;
  auto *disk_data = new char[PAGE_SIZE];
  for (size_t i = 0; i < pool_size; i++) {
    Page *page = &pages[i];
    page_id_t page_id = page->GetPageId();

    if (page_id != INVALID_PAGE_ID) {
      bustub_instance->disk_manager_->ReadPage(page_id, disk_data);
      if (std::memcmp(disk_data, page->GetData(), PAGE_SIZE) != 0) {
        all_pages_match = false;
        break;
      }
    }
  }

  EXPECT_TRUE(all_pages_match);
  delete[] disk_data;

  // Verify all committed transactions flushed to disk
  lsn_t persistent_lsn = bustub_instance->log_manager_->GetPersistentLSN();
  lsn_t next_lsn = bustub_instance->log_manager_->GetNextLSN();
  EXPECT_EQ(persistent_lsn, (next_lsn - 1));

  // verify log was flushed and each page's LSN <= persistent lsn
  bool all_pages_lte = true;
  for (size_t i = 0; i < pool_size; i++) {
    Page *page = &pages[i];
    page_id_t page_id = page->GetPageId();

    if (page_id != INVALID_PAGE_ID && page->GetLSN() > persistent_lsn) {
      all_pages_lte = false;
      break;
    }
  }

  EXPECT_TRUE(all_pages_lte);

  delete txn;
  delete txn1;
  delete test_table;

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}
}  // namespace bustub
