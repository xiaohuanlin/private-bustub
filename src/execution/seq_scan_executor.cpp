//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {
    auto catalog = exec_ctx_->GetCatalog();
    table_meta_ = catalog->GetTable(plan_->GetTableOid());
    table_iter_ = new TableIterator(table_meta_->table_->Begin(exec_ctx_->GetTransaction()));
}

void SeqScanExecutor::Init() {
}

bool SeqScanExecutor::Next(Tuple *tuple) {
    auto predicate = plan_->GetPredicate();
    do {
        if (*table_iter_ == table_meta_->table_->End()) {
            return false;
        }

        const Schema *schema = plan_->OutputSchema();
        if (schema) {
            std::vector<Value> res;
            for (auto &col: schema->GetColumns()) {
                auto value = col.GetExpr()->Evaluate(&(*(*table_iter_)), schema);
                res.push_back(value);
            }
            *tuple = Tuple(res, schema);
        } else {
            *tuple = **table_iter_;
        };

        ++(*table_iter_);
        if (!predicate) {
            return true;
        }
    } while (!predicate->Evaluate(tuple, &table_meta_->schema_).GetAs<bool>());
    return true;
}

SeqScanExecutor::~SeqScanExecutor() {
    delete table_iter_;
}

}  // namespace bustub
