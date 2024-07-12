//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * { 
  Page *page;
  frame_id_t frame_id = -1;
  std::scoped_lock lock(latch_);

  // pick the replacement frame from free list
  if (!free_list_.empty()) {
    // free_list_ last element
    frame_id = free_list_.back();
    free_list_.pop_back();
    page = pages_ + frame_id;
  } else {
    // pick the replacement frame from replacer_
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    page = pages_ + frame_id;
  }

  // If the replacement frame has a dirty page, write it back to the disk first
  if ( page -> IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    // ! clean
    page->is_dirty_ = false;
  }
  // reset the memory and metadata for the new page
  *page_id = AllocatePage();
  page_table_.erase(page->GetPageId());
  page_table_.emplace(*page_id, frame_id);
  
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  // reset the memory and metadata for the new page
  page->ResetMemory();
  // uodate replacer_ for lruk
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  
  std::scoped_lock lock(latch_);
  
  if (page_table_.find(page_id) != page_table_.end()) {
    // page in page_table_
    auto frame_id = page_table_[page_id];
    auto page = pages_ + frame_id;
    // replacer
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    // ! update pin count
    page->pin_count_ += 1;
    return page;
  }
  // Newpage 
  Page *page;
  frame_id_t frame_id = -1;

  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
    page = pages_ + frame_id;
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    page = pages_ + frame_id;
  }
  if (page->IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }

  page_table_.erase(page->GetPageId());
  page_table_.emplace(page_id, frame_id);
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->ResetMemory();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::scoped_lock lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  auto page = pages_ + frame_id;
  
  page->is_dirty_ = is_dirty||page->is_dirty_;
  // if pin count is 0
  if (page->GetPinCount() == 0) {
    return false;
  }
  // pin-1
  page->pin_count_ -= 1;
 
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::scoped_lock lock(latch_);
  
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  
  auto page = pages_ + page_table_[page_id];
  
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
  future.get();
  
  page->is_dirty_ = false;
  return true;

}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock lock(latch_);
  for (size_t current_size = 0; current_size < pool_size_; current_size++) {
    // 获得page_id在缓冲池中的位置
    auto page = pages_ + current_size;
    if (page->GetPageId() == INVALID_PAGE_ID) {
      continue;
    }
    // 和flush方法一样
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
    future.get();
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return true;
  }
  std::scoped_lock lock(latch_);
  // 如果页面存在
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    auto page = pages_ + frame_id;
    // 如果页面用着呢
    if (page->GetPinCount() > 0) {
      return false;
    }
    // 删除页面
    page_table_.erase(page_id);
    free_list_.push_back(frame_id);
    replacer_->Remove(frame_id);
    // 把内存该清的清，page的参数该换的换
    page->ResetMemory();
    page->page_id_ = INVALID_PAGE_ID;
    page->is_dirty_ = false;
    page->pin_count_ = 0;
  }
  // 注释里要求的：调用DeallocatePage()来模仿在磁盘上释放页面。
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { 
  auto page = FetchPage(page_id);
  return {this, page}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { 
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { 
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page}; }

}  // namespace bustub
