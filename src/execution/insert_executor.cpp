//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
        auto catalog = exec_ctx_->GetCatalog();
        table_meta_ = catalog->GetTable(plan_->TableOid());
    }

const Schema *InsertExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void InsertExecutor::Init() {}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple) {
    RID rid;
    bool res;
    if (plan_->IsRawInsert()) {
        for (auto &item: plan_->RawValues()) {
            auto insert_tuple = Tuple(item, &table_meta_->schema_);
            res = table_meta_->table_->InsertTuple(insert_tuple , &rid, exec_ctx_->GetTransaction());
            if (!res) {
                return false;
            }
        }
    } else if (child_executor_) {
        Tuple insert_tuple;
        while (child_executor_->Next(&insert_tuple)) {
            res = table_meta_->table_->InsertTuple(insert_tuple, &rid, exec_ctx_->GetTransaction());
            if (!res) {
                return false;
            }
        }
    }
    return true;
}

}  // namespace bustub
