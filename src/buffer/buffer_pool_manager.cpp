//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  frame_id_t frame_id;
  std::unordered_map<page_id_t, frame_id_t>::iterator frame_iter;
  if ((frame_iter = page_table_.find(page_id)) != page_table_.end()) {
    std::lock_guard<std::mutex> lg(latch_);
    replacer_->Pin(frame_iter->second);
    pages_[frame_iter->second].pin_count_++;
    return &pages_[frame_iter->second];
  }

  {
    std::lock_guard<std::mutex> lg(latch_);
    if (!free_list_.empty()) {
      frame_id = free_list_.back();
      free_list_.pop_back();
    } else {
      if (!replacer_->Victim(&frame_id)) {
        return nullptr;
      }
    }
  }

  if (pages_[frame_id].IsDirty()) {
    FlushPage(pages_[frame_id].GetPageId());
  }

  {
    std::lock_guard<std::mutex> lg(latch_);
    page_table_.erase(pages_[frame_id].GetPageId());

    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = page_id;
    page_table_[page_id] = frame_id;
  }

  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  return &pages_[frame_id];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lg(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPageId() == page_id) {
      pages_[i].is_dirty_ = is_dirty;
      pages_[i].pin_count_--;
      if (pages_[i].GetPinCount() <= 0) {
        replacer_->Unpin(i);
      }
      return true;
    }
  }
  return false;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lg(latch_);
  frame_id_t frame_id;
  std::unordered_map<page_id_t, frame_id_t>::iterator frame_iter;
  if ((frame_iter = page_table_.find(page_id)) != page_table_.end()) {
    frame_id = frame_iter->second;
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    return true;
  }
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  bool all_pined = true;
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPinCount() == 0) {
      all_pined = false;
      break;
    }
  }
  if (all_pined) {
    return nullptr;
  }

  frame_id_t frame_id;
  {
    std::lock_guard<std::mutex> lg(latch_);
    if (!free_list_.empty()) {
      frame_id = free_list_.back();
      free_list_.pop_back();
    } else {
      if (!replacer_->Victim(&frame_id)) {
        return nullptr;
      }
    }
  }

  if (pages_[frame_id].IsDirty()) {
    FlushPage(pages_[frame_id].GetPageId());
  }

  {
    std::lock_guard<std::mutex> lg(latch_);
    *page_id = disk_manager_->AllocatePage();

    page_table_.erase(pages_[frame_id].GetPageId());
    page_table_[*page_id] = frame_id;
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = *page_id;
  }

  return &pages_[frame_id];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::unordered_map<page_id_t, frame_id_t>::iterator frame_iter;
  if ((frame_iter = page_table_.find(page_id)) == page_table_.end()) {
    return true;
  }

  std::lock_guard<std::mutex> lg(latch_);
  if (pages_[frame_iter->first].GetPinCount() > 0) {
    return false;
  }

  disk_manager_->DeallocatePage(page_id);
  free_list_.push_back(frame_iter->second);
  page_table_.erase(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> lg(latch_);
  for (auto &item : page_table_) {
    FlushPage(item.first);
  }
}

}  // namespace bustub
