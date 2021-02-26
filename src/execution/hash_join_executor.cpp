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
    auto bfm = exec_ctx_->GetBufferPoolManager();
    page_id_t cur_page_id;
    bool first = true;
    TmpTuplePage *cur_page;
    const uint32_t PAGE_SIZE = 4096;

    while (left_->Next(&tuple)) {
        TmpTuple tmp_tuple(0, 0);
        // insert tuple to get tmp_tuple
        if (first) {
            first = false;
            bfm->NewPage(&cur_page_id);
            cur_page = reinterpret_cast<TmpTuplePage*>(bfm->FetchPage(cur_page_id));
            cur_page->Init(cur_page_id, PAGE_SIZE);
        }

        if (!cur_page->Insert(tuple, &tmp_tuple)) {
            // current page is enough, turn to next page
            bfm->NewPage(&cur_page_id);
            cur_page = reinterpret_cast<TmpTuplePage*>(bfm->FetchPage(cur_page_id));
            cur_page->Init(cur_page_id, PAGE_SIZE);
            cur_page->Insert(tuple, &tmp_tuple);
        }

        auto hash_key = HashValues(&tuple, left_->GetOutputSchema(), plan_->GetLeftKeys());
        std::cout << hash_key << std::endl;
        jht_.Insert(trans, hash_key, tmp_tuple);
        std::cout << "insert tmp: " << tmp_tuple.GetPageId() << " " << tmp_tuple.GetOffset() << " tuple " << tuple.ToString(left_->GetOutputSchema()) << std::endl;
    }

    right_->Init();
}

bool HashJoinExecutor::iter_to_next_key() {
    if (!right_->Next(&right_tuple_)) {
        return false;
    }

    htk_ = HashValues(&right_tuple_, right_->GetOutputSchema(), plan_->GetRightKeys());
    htk_idx_ = -1;
    return true;
}

bool HashJoinExecutor::Next(Tuple *tuple) {
    Tuple left_tuple;
    auto trans = exec_ctx_->GetTransaction();
    auto predicate = plan_->Predicate();
    auto bfm = exec_ctx_->GetBufferPoolManager();

    do {
        if (init_) {
            // init hash key
            init_ = false;
            if (!iter_to_next_key()) {
                return false;
            }
        }

        std::vector<TmpTuple> tuples;

        htk_idx_ += 1;
        jht_.GetValue(trans, htk_, &tuples);

        while (tuples.size() <= htk_idx_) {
            if (!iter_to_next_key()) {
                return false;
            }
            htk_idx_ += 1;
            // we should keep the tuples empty
            tuples.clear();
            jht_.GetValue(trans, htk_, &tuples);
        }

        auto tmp_tuple = tuples[htk_idx_];
        auto cur_page = reinterpret_cast<TmpTuplePage*>(bfm->FetchPage(tmp_tuple.GetPageId()));
        cur_page->Get(&tmp_tuple, &left_tuple);
        std::cout << "get tmp: " << tmp_tuple.GetPageId() << " " << tmp_tuple.GetOffset() << " tuple " << left_tuple.ToString(left_->GetOutputSchema()) << std::endl;

        // generate output
        const Schema *schema = plan_->OutputSchema();

        std::vector<Value> res;
        for (auto &col: schema->GetColumns()) {
            auto value = col.GetExpr()->EvaluateJoin(&left_tuple, left_->GetOutputSchema(), &right_tuple_, right_->GetOutputSchema());
            res.push_back(value);
        }

        *tuple = Tuple(res, plan_->OutputSchema());
        std::cout << "general " << tuple->ToString(plan_->OutputSchema()) << std::endl;
    } while (!predicate->EvaluateJoin(&left_tuple, left_->GetOutputSchema(), &right_tuple_, right_->GetOutputSchema()).GetAs<bool>());
    return true;
}
}  // namespace bustub
