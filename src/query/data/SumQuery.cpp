//
// Created by Jerry on 2022/11/3.
//

#include "SumQuery.h"

#include <iostream>
#include <thread>

#include "../../db/Database.h"

constexpr const char *SumQuery::qname;

void SumQuery::threads(int total, int index, Table* table , std::vector< std::pair<size_t, int> > &sumResults)  noexcept 
// index from 0 to total-1
{
  auto it = table->begin() + slice * index;
  while (true) {
    if (it + slice >= table->end())
      break;
    for (int i = 0; i < slice; i++) {
      if (this->evalCondition(*(it + i))) {
        for (auto &sumResult : sumResults) {
          sumResult.second += (*(it + i))[sumResult.first];
        }
      }
    }
    it += slice * (total);
  }
  for (int i = 0; i < slice; i++) {
    if (it + i >= table->end())
      break;
    if (this->evalCondition(*(it + i))) {
      for (auto &sumResult : sumResults) {
        sumResult.second += (*(it + i))[sumResult.first];
      }
    }
  }
}

QueryResult::Ptr SumQuery::execute(int threadsNum) noexcept  {
  using namespace std;
  if (this->operands.empty())
    return make_unique<ErrorMsgResult>(
        qname, this->targetTable.c_str(),
        "Invalid number of operands (? operands)."_f % operands.size());
  Database &db = Database::getInstance();
  // try {
    auto &table = db[this->targetTable];

    auto result = initCondition(table);

    if (result.second) {
      if (threadsNum == 1 || table.size() < (size_t)threadsNum * 100){
        vector< pair<size_t, int> > sumResults;
        for (auto it = this->operands.begin(); it != operands.end(); ++it)
          sumResults.emplace_back(table.getFieldIndex(*it), 0);
        for (auto it : table) {
          if (this->evalCondition(it)) {
            for (auto & sumResult : sumResults)
              sumResult.second += it[sumResult.first];
          }
        }
        vector<int> answer;
        answer.reserve(sumResults.size());
        for (auto & sumResult : sumResults)
          answer.push_back((int)sumResult.second);
        return make_unique<AnswerMsgResult>(answer);
      } else {
        std::vector<std::thread> threadsV;
        threadsV.reserve(static_cast<size_t>(threadsNum));
        std::vector< vector< pair<size_t, int> > > sumResultThreads;
        sumResultThreads.reserve(static_cast<size_t>(threadsNum));
        for (int i = 0; i < threadsNum; ++i)
        {
          sumResultThreads.emplace_back();
          for (auto it = this->operands.begin(); it != operands.end(); ++it)
            sumResultThreads[static_cast<size_t>(i)].emplace_back(table.getFieldIndex(*it), 0);
        }
        std::vector< pair<size_t, int> > sumResults;
        for (auto it = this->operands.begin(); it != operands.end(); ++it)
          sumResults.emplace_back(table.getFieldIndex(*it), 0);
        for (int i = 0; i < threadsNum; ++i)
        {
          threadsV.emplace_back(&SumQuery::threads, this, threadsNum, i, &table, std::ref(sumResultThreads[static_cast<size_t>(i)]));
        }
        for (int i = 0; i < threadsNum; ++i)
        {
          threadsV[static_cast<size_t>(i)].join();
        }
        for (const auto& sumResultT : sumResultThreads) {
          auto sumResultT_IT = sumResultT.begin();
          auto sumResult_IT = sumResults.begin();
          while (sumResultT_IT != sumResultT.end()){
            sumResult_IT->second += sumResultT_IT->second;
            sumResultT_IT++;
            sumResult_IT++;
          }
        }
        vector<int> answer;
        answer.reserve(sumResults.size());
        for (auto & sumResult : sumResults)
          answer.push_back((int)sumResult.second);
        return make_unique<AnswerMsgResult>(answer);
      }
    }
    else{
      vector<int> answer;
      answer.reserve(operands.size());
      for (auto it = this->operands.begin(); it != operands.end(); ++it)
        answer.emplace_back(0);
      return make_unique<AnswerMsgResult>(answer);
    }
  // } catch (const TableNameNotFound &e) {
  //   return make_unique<ErrorMsgResult>(qname, this->targetTable,
  //                                      "No such table."s);
  // } catch (const IllFormedQueryCondition &e) {
  //   return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  // } catch (const invalid_argument &e) {
  //   // Cannot convert operand to string
  //   return make_unique<ErrorMsgResult>(qname, this->targetTable,
  //                                      "Unknown error '?'"_f % e.what());
  // } catch (const exception &e) {
  //   return make_unique<ErrorMsgResult>(qname, this->targetTable,
  //                                      "Unkonwn error '?'."_f % e.what());
  // }
}

std::string SumQuery::toString() noexcept  {
  return "QUERY = SUM " + this->targetTable + "\"";
}
