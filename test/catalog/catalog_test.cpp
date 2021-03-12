//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/simple_catalog.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new SimpleCatalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);

  EXPECT_EQ(catalog->GetTable(table_metadata->oid_), table_metadata);
  EXPECT_EQ(catalog->GetTable(table_name), table_metadata);

  delete catalog;
  delete bpm;
  delete disk_manager;
}

static unsigned int count;
pthread_mutex_t lock;

void *create(void *v) {
  auto catalog = reinterpret_cast<SimpleCatalog *>(v);
  pthread_mutex_lock(&lock);
  std::string table_name = std::to_string(count++);
  pthread_mutex_unlock(&lock);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);
  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);

  EXPECT_EQ(catalog->GetTable(table_metadata->oid_), table_metadata);
  EXPECT_EQ(catalog->GetTable(table_name), table_metadata);

  return nullptr;
}

TEST(CatalogTest, ConcurrentTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new SimpleCatalog(bpm, nullptr, nullptr);

  int tid_len = 10;
  pthread_t tid[tid_len];
  for (int i = 0; i < tid_len; i++) {
    pthread_create(&tid[i], nullptr, create, reinterpret_cast<void *>(catalog));
  }

  for (int i = 0; i < tid_len; i++) {
    pthread_join(tid[i], nullptr);
  }

  delete catalog;
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
