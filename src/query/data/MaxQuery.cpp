//
// Created by Jerry on 2022/11/4.
//

#include "MaxQuery.h"
#include "../../db/Database.h"
#include "../../utils/threadPool.h"
#include <iostream>
#include <thread>
const int slice = 5; 
constexpr const char *MaxQuery::qname;
extern ThreadPool threadPool;

void MaxQuery::threads(MaxQuery *th, int total, int index, Table *table,
                       std::vector<int> &maxResults, std::vector<bool>::reference &firstVal) noexcept 
// index from 0 to total-1
{
  auto it = table->begin() + slice * index;
  while (true) {
    if (it + slice >= table->end())
      break;
    for (int i = 0; i < slice; i++) {
      if (th->evalCondition(*(it + i))) {
        for (size_t j = 0; j < maxResults.size(); j++) {
          auto &maxResult = maxResults[j];
          if (firstVal) {
            // result_mtx.lock();
            maxResult = (*(it + i))[j];
            // result_mtx.unlock();
          } else maxResult = std::max(maxResult,(*(it + i))[j]);

            // result_mtx.unlock();
          // }
        }
        firstVal = false;
      }
    }
    it += slice * (total);
  }
  for (int i = 0; i < slice; i++) {
    if (it + i >= table->end())
      break;
    if (th->evalCondition(*(it + i))) {
      for (size_t j = 0; j < maxResults.size(); j++) {
        auto &maxResult = maxResults[j];
        if (firstVal) {
          // result_mtx.lock();
          maxResult = (*(it + i))[j];
          // result_mtx.unlock();
        } else 
            maxResult = std::max(maxResult,(*(it + i))[j]);
          // }
          // result_mtx.unlock();
        // }
      }
      firstVal = false;
    }
  }
}

QueryResult::Ptr MaxQuery::execute(int threadsNum) noexcept  {
  threadsNum = 1;
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
      if (threadsNum == 1 || table.size() < (size_t)threadsNum * 100) {
        vector<pair<size_t, int>> maxResults;
        bool firstVal = true;
        for (auto it = this->operands.begin(); it != operands.end(); ++it)
          maxResults.emplace_back(table.getFieldIndex(*it), 0);
        for (auto it : table) {
          if (this->evalCondition(it)) {
            for (auto &maxResult : maxResults) {
              if (firstVal) {
                maxResult.second = it[maxResult.first];
              } else if (it[maxResult.first] > maxResult.second) {
                maxResult.second = it[maxResult.first];
              }
            }
            firstVal = false;
          }
        }
        if (firstVal)
          return make_unique<NullQueryResult>();
        vector<int> answer;
        answer.reserve(maxResults.size());
        for (auto &maxResult : maxResults)
          answer.push_back((int)maxResult.second);
        return make_unique<AnswerMsgResult>(answer);
      } else {
        std::vector<std::future<void>> threadsV;
        threadsV.reserve(static_cast<size_t>(threadsNum));
        /*std::vector< vector< pair<size_t, int> > > maxResultThreads;
        maxResultThreads.reserve(static_cast<size_t>(threadsNum));
        for (auto &maxResultT : maxResultThreads) { // initialize results
          for (auto it = this->operands.begin(); it != operands.end(); ++it)
            maxResultT.emplace_back(table.getFieldIndex(*it), 0);
        }*/
        std::vector<std::vector<int>> threadResult;
        std::vector<bool> threadResultFirst(threadsNum,true);
        std::vector<int> maxResults(operands.size(),0);
        std::vector<int> ans;
        // for (auto it = this->operands.begin(); it != operands.end(); ++it)
        //   {maxResults.emplace_back(0);}

        threadResult.resize(static_cast<size_t>(threadsNum), maxResults);
        
        bool firstVal = true;
        for (int i = 0; i < threadsNum; ++i) {
          threadsV.emplace_back(threadPool.enqueueTask(
              threads, this, threadsNum, i, &table, threadResult[i],threadResultFirst[i]));
        }
        for (int i = 0; i < threadsNum; ++i) {
          threadsV[static_cast<size_t>(i)].get();
          if (threadResultFirst[i])
          {
            if (firstVal)
            {
              ans = threadResult[i];
              firstVal=false;
            }
            else {
              for (size_t j=0;j<operands.size();j++)
              {
                ans[j]=max(ans[j],threadResult[i][j]);
              }
            }
          }
        }
        if (firstVal)
          return make_unique<NullQueryResult>();
        // vector<int> answer;
        // answer.reserve(maxResults.size());
        // for (auto &maxResult : maxResults)
        //   answer.push_back((int)maxResult.second);
        return make_unique<AnswerMsgResult>(ans);
      }
    } else {
      return make_unique<NullQueryResult>();
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

std::string MaxQuery::toString() noexcept  {
  return "QUERY = MAX " + this->targetTable + "\"";
}
