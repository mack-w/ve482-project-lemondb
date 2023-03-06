//
// Created by wznmickey on 20221026
//

#include "DeleteQuery.h"

#include "../../db/Database.h"
#include "../../utils/threadPool.h"
#include "../QueryResult.h"
#include <algorithm>
#include <atomic>
#include <future>
#include <iostream>
#include <thread>

extern ThreadPool threadPool;

constexpr const char *DeleteQuery::qname;
const int slice = 5; // slice for each thread

void DeleteQuery::threads(DeleteQuery *th, int total, int index, Table *table,
                          Table::DeleteQueue *queue) noexcept 
// index from 0 to total-1
{
  auto it = table->begin() + slice * index;
  while (true) {
    if (it + slice >= table->end())
      break;
    for (int i = 0; i < slice; i++) {
      if (th->evalCondition(*(it + i))) {
        queue->push(it+i);
      }
    }
    it += slice * (total);
  }
  for (int i = 0; i < slice; i++) {
    if (it + i >= table->end())
      break;
    if (th->evalCondition(*(it + i))) {
      queue->push(it+i);
    }
  }
}

QueryResult::Ptr DeleteQuery::execute(int threadsNum) noexcept {
  using namespace std;
  Database &db = Database::getInstance();
  // try {
    auto &table = db[this->targetTable];
    auto result = initCondition_(table);
    if (result.second==-1) {
      auto count=table.size();
      table.clear();
      return make_unique<RecordCountResult>(count);
    }
    size_t count = 0;
    threadsNum = std::min((int)table.size() / slice / 10, threadsNum);
    threadsNum = std::max(1, threadsNum);
    std::vector<std::future<void>> threadsV;
    threadsV.reserve((size_t)threadsNum);
    Table::DeleteQueue queue(&table, table.size());
    if (result.second==0) {
      for (size_t i = 0; i < (size_t)threadsNum; i++) {
        threadsV.emplace_back(threadPool.enqueueTask(threads, this, threadsNum,
                                                     i, &table, &queue));

        // std::cout << "Affected " << " rows" << std::endl;
      }
      for (size_t i=0;i<(size_t)threadsNum;i++)
      {
        threadsV[i].get();
      }
      queue.sort();
      count = queue.doDelete();}
      return make_unique<RecordCountResult>(count);
    // }
    // catch (const TableNameNotFound &e) {
    //   return make_unique<ErrorMsgResult>(qname, this->targetTable,
    //                                      "No such table."s);
    // }
    // catch (const IllFormedQueryCondition &e) {
    //   return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    // }
    // catch (const invalid_argument &e) {
    //   // Cannot convert operand to string
    //   return make_unique<ErrorMsgResult>(qname, this->targetTable,
    //                                      "Unknown error '?'"_f % e.what());
    // }
    // catch (const exception &e) {
    //   return make_unique<ErrorMsgResult>(qname, this->targetTable,
    //                                      "Unkonwn error '?'."_f % e.what());
    // }
  }

  std::string DeleteQuery::toString()  noexcept {
    return "QUERY = DELETE " + this->targetTable + "\"";
  }
