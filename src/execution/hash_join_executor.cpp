//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left, std::unique_ptr<AbstractExecutor> &&right)
    : AbstractExecutor(exec_ctx), plan_(plan), jht_("tmp", exec_ctx_->GetBufferPoolManager(), jht_comp_, jht_num_buckets_, jht_hash_fn_), init_(true), left_(std::move(left)), right_(std::move(right)) {
    }

void HashJoinExecutor::Init() {
    left_->Init();
    Tuple tuple;
    auto trans = exec_ctx_->GetTransaction();
    while (left_->Next(&tuple)) {
        auto hash_key = HashValues(&tuple, left_->GetOutputSchema(), plan_->GetLeftKeys());
        jht_.Insert(trans, hash_key, tuple);
    }

    right_->Init();
}

bool HashJoinExecutor::iter_to_next_key() {
    Tuple right_tuple;
    if (!right_->Next(&right_tuple)) {
        return false;
    }

    htk_ = HashValues(&right_tuple, right_->GetOutputSchema(), plan_->GetRightKeys());
    htk_idx_ = -1;
    return true;
}

bool HashJoinExecutor::Next(Tuple *tuple) {
    Tuple left_tuple, right_tuple;
    auto trans = exec_ctx_->GetTransaction();
    auto predicate = plan_->Predicate();

    do {
        if (init_) {
            // init hash key
            init_ = false;
            if (!iter_to_next_key()) {
                return false;
            }
        }

        std::vector<Tuple> res;

        htk_idx_ += 1;
        jht_.GetValue(trans, htk_, &res);

        while (res.size() <= htk_idx_) {
            if (!iter_to_next_key()) {
                return false;
            }
            htk_idx_ += 1;
            jht_.GetValue(trans, htk_, &res);
        }

        *tuple = res[htk_idx_];
    } while (!predicate->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>());
    return true;
}
}  // namespace bustub
