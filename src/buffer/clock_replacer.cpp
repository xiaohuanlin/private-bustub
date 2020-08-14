//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : total(num_pages) {
  current = units.begin();
}

ClockReplacer::~ClockReplacer() {}

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  // Starting from the current position of clock hand,
  // find the first frame that is both in the `ClockReplacer` and
  // with its ref flag set to false. If a frame is in the `ClockReplacer`,
  // but its ref flag is set to true, change it to false instead.
  // This should be the only method that updates the clock hand.
  if (units.empty()) {
    return false;
  }

  while (true) {
    std::lock_guard<std::mutex> lk_guard(unit_lock);
    if (current == units.cend()) {
      current = units.begin();
    }

    if (!(*current)->ref_count) {
      *frame_id = (*current)->frame_id;
      units.erase(current++);
      return true;
    } else {
      (*current)->ref_count = true;
      current++;
    }
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  // This method should be called after a page is pinned to a frame
  // in the BufferPoolManager. It should remove the frame containing the pinned page
  // from the ClockReplacer.
  std::lock_guard<std::mutex> lk_guard(unit_lock);

  auto item =
      std::find_if(units.cbegin(), units.cend(), [=](const std::shared_ptr<Unit> u) { return u->frame_id == frame_id; });
      
  if (item != units.cend()) {
    if (current == item) {
      current++;
      if (current == units.cend()) {
        current = units.begin();
      }
    }

    units.erase(item);
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  // This method should be called when the pin_count of a page becomes 0.
  // This method should add the frame containing the unpinned page to the ClockReplacer.
  if (units.size() >= total) {
    return;
  }
  std::lock_guard<std::mutex> lk_guard(unit_lock);
  auto item =
      std::find_if(units.cbegin(), units.cend(), [=](const std::shared_ptr<Unit> u) { return u->frame_id == frame_id; });

  if (item == units.cend()) {
    units.insert(current, std::shared_ptr<Unit>(new Unit(frame_id, false)));
  }
}

size_t ClockReplacer::Size() {
  // This method returns the number of frames that are currently in the ClockReplacer.
  return units.size();
}

}  // namespace bustub
