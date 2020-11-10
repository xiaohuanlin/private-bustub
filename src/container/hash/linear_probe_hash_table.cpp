//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)), size_(0) {
  // todo: find table by name
  // todo: how to utilize transaction?
  Page *header = buffer_pool_manager_->NewPage(&header_page_id_);

  auto header_page = reinterpret_cast<HashTableHeaderPage *>(header->GetData());
  header_page->SetPageId(header_page_id_);
  header_page->SetSize(BLOCK_ARRAY_SIZE);
  for (size_t i = 0; i < num_buckets; i++) {
    page_id_t block_page_id;
    buffer_pool_manager_->NewPage(&block_page_id);
    header_page->AddBlockPageId(block_page_id);
    buffer_pool_manager->UnpinPage(block_page_id, true);
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  HashTableHeaderPage *header_page = loadHeaderPage(false, true);

  auto hash_v = hash_fn_.GetHash(key);
  page_id_t page_idx = hash_v % header_page->NumBlocks();
  page_id_t ori_page_idx = page_idx;
  slot_offset_t offset = hash_v % header_page->GetSize();
  slot_offset_t ori_offset = offset;

  auto block_page = loadBlockPage(header_page, page_idx, false, true);

  while (block_page->IsOccupied(offset)) {
    if (block_page->IsReadable(offset) && comparator_(block_page->KeyAt(offset), key) == 0) {
      result->push_back(block_page->ValueAt(offset));
    }
    gotoNextPosition(header_page, &block_page, &page_idx, &offset, false, true);
    if (page_idx == ori_page_idx && offset == ori_offset) {
      // search all data
      break;
    }
  }
  freePage(reinterpret_cast<Page *>(block_page), header_page->GetBlockPageId(page_idx), false, true, false);
  freePage(reinterpret_cast<Page *>(header_page), header_page_id_, false, true, false);
  return !result->empty();
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  std::vector<ValueType> result;
  GetValue(transaction, key, &result);
  for (auto res : result) {
    if (res == value) {
      // not allowed to insert the same key-value pair
      table_latch_.RUnlock();
      return false;
    }
  }

  HashTableHeaderPage *header_page = loadHeaderPage(false, true);

  auto hash_v = hash_fn_.GetHash(key);
  page_id_t page_idx = hash_v % header_page->NumBlocks();
  page_id_t ori_page_idx = page_idx;
  slot_offset_t offset = hash_v % header_page->GetSize();
  slot_offset_t ori_offset = offset;

  auto block_page = loadBlockPage(header_page, page_idx, true, false);

  while (block_page->IsReadable(offset)) {
    gotoNextPosition(header_page, &block_page, &page_idx, &offset, true, false);
    if (page_idx == ori_page_idx && offset == ori_offset) {
      freePage(reinterpret_cast<Page *>(block_page), header_page->GetBlockPageId(page_idx), true, false, false);
      freePage(reinterpret_cast<Page *>(header_page), header_page_id_, false, true, false);
      table_latch_.RUnlock();

      // all entries are full, so resize the table
      Resize(header_page->NumBlocks());
      return Insert(transaction, key, value);
    }
  }

  block_page->Insert(offset, key, value);
  size_++;

  freePage(reinterpret_cast<Page *>(block_page), header_page->GetBlockPageId(page_idx), true, false, true);
  freePage(reinterpret_cast<Page *>(header_page), header_page_id_, false, true, false);
  table_latch_.RUnlock();
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableHeaderPage *header_page = loadHeaderPage(false, true);

  auto hash_v = hash_fn_.GetHash(key);
  page_id_t page_idx = hash_v % header_page->NumBlocks();
  page_id_t ori_page_idx = page_idx;
  slot_offset_t offset = hash_v % header_page->GetSize();
  slot_offset_t ori_offset = offset;

  auto block_page = loadBlockPage(header_page, page_idx, true, false);

  bool removed = false;
  while (block_page->IsOccupied(offset)) {
    if (block_page->IsReadable(offset) && comparator_(block_page->KeyAt(offset), key) == 0 &&
        block_page->ValueAt(offset) == value) {
      block_page->Remove(offset);
      removed = true;
      size_--;
      break;
    }

    gotoNextPosition(header_page, &block_page, &page_idx, &offset, true, false);
    if (page_idx == ori_page_idx && offset == ori_offset) {
      // search all data
      break;
    }
  }

  freePage(reinterpret_cast<Page *>(block_page), header_page->GetBlockPageId(page_idx), true, false, true);
  freePage(reinterpret_cast<Page *>(header_page), header_page_id_, false, true, false);

  table_latch_.RUnlock();
  return removed;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();
  HashTableHeaderPage *header_page = loadHeaderPage(false, true);
  if (header_page->NumBlocks() == 2 * initial_size) {
    freePage(reinterpret_cast<Page *>(header_page), header_page_id_, false, true, false);
    table_latch_.WUnlock();
    return;
  }

  page_id_t page_idx = 0;
  slot_offset_t offset = 0;

  auto block_page = loadBlockPage(header_page, page_idx, false, true);
  auto new_table = new LinearProbeHashTable("tmp", buffer_pool_manager_, comparator_, 2 * initial_size, hash_fn_);
  do {
    if (block_page->IsReadable(offset)) {
      new_table->Insert(nullptr, block_page->KeyAt(offset), block_page->ValueAt(offset));
    }
    gotoNextPosition(header_page, &block_page, &page_idx, &offset, false, true);
  } while (!(page_idx == 0 && offset == 0));

  freePage(reinterpret_cast<Page *>(header_page), header_page_id_, false, true, false);
  header_page_id_ = new_table->header_page_id_;
  delete new_table;
  table_latch_.WUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableHeaderPage *HASH_TABLE_TYPE::loadHeaderPage(bool wlock, bool rlock) {
  Page *header = buffer_pool_manager_->FetchPage(header_page_id_);
  if (header == nullptr) {
    return nullptr;
  }

  if (rlock) {
    header->RLatch();
  } else if (wlock) {
    header->WLatch();
  }
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(header->GetData());
  return header_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableBlockPage<KeyType, ValueType, KeyComparator> *HASH_TABLE_TYPE::loadBlockPage(HashTableHeaderPage *header_page,
                                                                                      size_t page_idx, bool wlock,
                                                                                      bool rlock) {
  page_id_t block_page_id = header_page->GetBlockPageId(page_idx);
  Page *block = buffer_pool_manager_->FetchPage(block_page_id);
  if (block == nullptr) {
    return nullptr;
  }
  if (rlock) {
    block->RLatch();
  } else if (wlock) {
    block->WLatch();
  }

  auto block_page = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block->GetData());
  return block_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::freePage(Page *page, page_id_t page_id, bool wlock, bool rlock, bool dirty) {
  buffer_pool_manager_->UnpinPage(page_id, dirty);
  if (rlock) {
    page->RUnlatch();
  } else if (wlock) {
    page->WUnlatch();
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::gotoNextPosition(HashTableHeaderPage *header_page,
                                       HashTableBlockPage<KeyType, ValueType, KeyComparator> **block_page_p,
                                       page_id_t *page_id_p, slot_offset_t *offset_p, bool wlock, bool rlock) {
  ++(*offset_p);

  if (*offset_p >= header_page->GetSize()) {
    // finish this page, and turn into another page
    freePage(reinterpret_cast<Page *>(*block_page_p), header_page->GetBlockPageId(*page_id_p), wlock, rlock, false);
    ++(*page_id_p);
    *offset_p = 0;

    // search to end, rewind to start page
    if (header_page->NumBlocks() == static_cast<size_t>(*page_id_p)) {
      *page_id_p = 0;
    }
    // find next block to search the result
    *block_page_p = loadBlockPage(header_page, *page_id_p, wlock, rlock);
  }
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return size_.load();
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
