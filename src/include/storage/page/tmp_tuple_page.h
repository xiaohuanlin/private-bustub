#pragma once

#include "storage/page/page.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

// To pass the test cases for this class, you must follow the existing TmpTuplePage format and implement the
// existing functions exactly as they are! It may be helpful to look at TablePage.
// Remember that this task is optional, you get full credit if you finish the next task.

/**
 * TmpTuplePage format:
 *
 * Sizes are in bytes.
 * | PageId (4) | LSN (4) | FreeSpace (4) | (free space) | TupleSize2 | TupleData2 | TupleSize1 | TupleData1 |
 *
 * We choose this format because DeserializeExpression expects to read Size followed by Data.
 */
class TmpTuplePage : public Page {
 public:
  void Init(page_id_t page_id, uint32_t page_size) {
    BUSTUB_ASSERT(page_size - SIZE_TABLE_PAGE_HEADER > 0, "page size too small");
    // Set the page ID.
    memcpy(GetData(), &page_id, sizeof(page_id));
    // Set the previous and next page IDs.
    SetFreeSpacePointer(page_size - SIZE_TABLE_PAGE_HEADER);
  }

  page_id_t GetTablePageId() { return INVALID_PAGE_ID; }

  uint32_t GetFreeSpacePointer() { return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_FREE_SPACE); }

  void SetFreeSpacePointer(uint32_t free_space_pointer) {
    memcpy(GetData() + OFFSET_FREE_SPACE, &free_space_pointer, sizeof(uint32_t));
  }

  uint32_t GetFreeSpaceRemaining() {
    return GetFreeSpacePointer();
  }

  bool Insert(const Tuple &tuple, TmpTuple *out) {
    BUSTUB_ASSERT(tuple.GetLength()> 0, "Cannot have empty tuples.");
    // If there is not enough space, then return false.
    size_t tuple_size = tuple.GetLength();
    if (GetFreeSpaceRemaining() < tuple_size + SIZE_TUPLE) {
      return false;
    }

    uint32_t free_remain = GetFreeSpaceRemaining() - tuple_size - SIZE_TUPLE;
    SetFreeSpacePointer(free_remain);

    size_t offset = free_remain + SIZE_TABLE_PAGE_HEADER;
    // set size
    memcpy(GetData() + offset, &tuple_size, sizeof(uint32_t));
    // set data
    memcpy(GetData() + offset + SIZE_TUPLE, tuple.GetData(), tuple.GetLength());

    *out = TmpTuple(GetPageId(), offset);
    return true;
  }

  bool Get(const TmpTuple *in, Tuple *out) {
    if (in->GetPageId() != GetPageId()) {
      return false;
    }

    uint32_t tuple_size = *reinterpret_cast<uint32_t *>(GetData() + in->GetOffset());

    out->size_ = tuple_size;
    if (out->allocated_) {
      delete[] out->data_;
    }
    out->data_ = new char[out->size_];
    memcpy(out->data_, GetData() + in->GetOffset() + SIZE_TUPLE, out->size_);
    out->allocated_ = true;
    return true;
  }

 private:
  static_assert(sizeof(page_id_t) == 4);

  static constexpr size_t SIZE_TABLE_PAGE_HEADER = 12;
  static constexpr size_t SIZE_TUPLE = 4;
  static constexpr size_t OFFSET_FREE_SPACE = 8;

  page_id_t page_id_;
};

}  // namespace bustub
