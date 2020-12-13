//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)), aht_(SimpleAggregationHashTable(plan->GetAggregates(), plan->GetAggregateTypes())), aht_iterator_(aht_.Begin()) {
    }

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

const Schema *AggregationExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void AggregationExecutor::Init() {
    Tuple tuple;
    while (child_->Next(&tuple)) {
        auto ag_k = MakeKey(&tuple);
        auto ag_v = MakeVal(&tuple);
        aht_.InsertCombine(ag_k, ag_v);
    }
    aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple) {
    auto predicate = plan_->GetHaving();

    do {
        if (aht_iterator_ == aht_.End()) {
            return false;
        }

        auto ak_k = aht_iterator_.Key();
        auto ak_v = aht_iterator_.Val();

        // create tuple according to output schema
        std::vector<Value> res;
        const Schema *schema = plan_->OutputSchema();
        for (auto &col: schema->GetColumns()) {
            auto value = col.GetExpr()->EvaluateAggregate(ak_k.group_bys_, ak_v.aggregates_);
            res.push_back(value);
        }

        *tuple = Tuple(res, plan_->OutputSchema());

        ++aht_iterator_;
        if (!predicate) {
            return true;
        }
    } while (!predicate->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>());
    return true;
}

}  // namespace bustub
