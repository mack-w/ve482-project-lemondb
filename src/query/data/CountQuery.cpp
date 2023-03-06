//
// Created by wznmcikey on 20221028
//

#include "CountQuery.h"

#include "../../db/Database.h"
#include "../../utils/threadPool.h"
#include <iostream>
#include <iterator>
#include <thread>
#include <vector>

extern ThreadPool threadPool;
constexpr const char *CountQuery::qname;

// const int threadsT = 5; // A fixed number to test.

const int slice = 5; // slice for each thread

void CountQuery::threads(CountQuery *th, int total, int index, Table *table,
                         size_t *ans) noexcept 
// index from 0 to total-1
{
  auto it = table->begin() + slice * index;
  while (true) {
    if (it + slice >= table->end())
      break;
    for (int i = 0; i < slice; i++) {
      if (th->evalCondition(*(it + i))) {
        (*ans)++;
      }
    }
    it += slice * (total);
  }
  for (int i = 0; i < slice; i++) {
    if (it + i >= table->end())
      break;
    if (th->evalCondition(*(it + i))) {
      (*ans)++;
    }
  }
}

QueryResult::Ptr CountQuery::execute(int threadsNum)  noexcept {
  using namespace std;
  Database &db = Database::getInstance();
  // try {
    auto &table = db[this->targetTable];
    size_t ans = 0;
    auto result = initCondition_(table);
    if (result.second == -1)
      return make_unique<AnswerMsgResult>(table.size());
    if (result.second == 0) {
      if (threadsNum == 1) {
        // auto result = initCondition(table);
        for (auto it = table.begin(); it != table.end(); ++it) {
          if (this->evalCondition(*it)) {
            ans++;
          }
        }
      } else {

        // vector<size_t> fieldsNeeded;// This stores the fields that need to
        // print for (auto it = this->operands.begin() + 1; it !=
        // operands.end();
        // ++ it)
        // //this->operands.begin() + 1 to skip leading KEY
        //   fieldsNeeded.push_back(table.getFieldIndex(*it));
        threadsNum = std::min((int)table.size() / slice / 10, threadsNum);
        threadsNum = std::max(1, threadsNum);
        std::vector<std::future<void>> threadsV;
        threadsV.reserve((size_t)threadsNum);

        std::vector<size_t> ansV((size_t)threadsNum, 0);

        // for (auto it = table.begin(); it != table.end(); ++it) {
        //   if (this->evalCondition(*it)) {
        //     ans++;
        //   }
        // }
        for (size_t i = 0; i < (size_t)threadsNum; i++) {
          threadsV.emplace_back(threadPool.enqueueTask(
              threads, this, threadsNum, i, &table, &ansV[i]));
          // threadsV.emplace_back(&CountQuery::threads,this,threadsNum,i,&table,&ansV[i]);
          // std::thread x();
          // threadsV.push_back(std::move(x));
        }
        for (size_t i = 0; i < (size_t)threadsNum; i++) {
          threadsV[i].get();
          ans += ansV[i];
        }
      }
    }
    // cout<<"ANSWER = "<<ans<<endl;
    return make_unique<AnswerMsgResult>(ans);
  // }

  // catch (const TableNameNotFound &e) {
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

std::string CountQuery::toString()  noexcept {
  return "QUERY = COUNT " + this->targetTable + "\"";
}