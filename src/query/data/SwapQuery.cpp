//
// Created by Mack on 22-11-06.
//

#include "SwapQuery.h"

#include <algorithm>
#include <thread>

#include "../../db/Database.h"
#include "../QueryResult.h"

constexpr const char *SwapQuery::qname;

void SwapQuery::threads(int total, int index, Table* table, size_t op1, size_t op2, size_t &rowCount) noexcept 
// index from 0 to total-1
{
  auto it = table->begin() + slice * index;
  while (true) {
    if (it + slice >= table->end())
      break;
    for (int i = 0; i < slice; i++) {
      if (this->evalCondition(*(it + i))) {
        std::swap((*(it + i))[op1], (*(it + i))[op2]);
        rowCount++;
      }
    }
    it += slice * (total);
  }
  for (int i = 0; i < slice; i++) {
    if (it + i >= table->end())
      break;
    if (this->evalCondition(*(it + i))) {
      std::swap((*(it + i))[op1], (*(it + i))[op2]);
      rowCount++;
    }
  }
}

QueryResult::Ptr SwapQuery::execute(int threadsNum) noexcept  {
  using namespace std;
  if (this->operands.size() != 2)
    return make_unique<ErrorMsgResult>(
        qname, this->targetTable.c_str(),
        "Invalid number of operands (? operands)."_f % operands.size());
  Database &db = Database::getInstance();
  Table::SizeType affectedRowCnt = 0;
  // try {
    // if (this->operands[0] == this->operands[1])
    //   return make_unique<RecordCountResult>(affectedRowCnt);
    auto &table = db[this->targetTable];
    auto result = initCondition(table);
    if (result.second) {
      if (threadsNum == 1 || table.size() < (size_t)threadsNum * 100) {
        for (auto datum : table) {
          if (this->evalCondition(datum)) {
            // can use std::swap here
            std::swap(datum[operands[0]], datum[operands[1]]);
            affectedRowCnt++;
          }
        }
      } else {
        std::vector<std::thread> threadsV;
        threadsV.reserve(static_cast<size_t>(threadsNum));
        std::vector<size_t> rowCountT;
        rowCountT.reserve(static_cast<size_t>(threadsNum));
        for (int i = 0; i < threadsNum; ++i) {
          rowCountT.emplace_back(0);
        }
        for (int i = 0; i < threadsNum; ++i) {
          threadsV.emplace_back(&SwapQuery::threads, this, threadsNum, i, &table, table.getFieldIndex(operands[0]), table.getFieldIndex(operands[1]), std::ref(rowCountT[static_cast<size_t>(i)]));
        }
        for (int i = 0; i < threadsNum; ++i) {
          threadsV[static_cast<size_t>(i)].join();
        }
        for (auto rowCount : rowCountT) {
          affectedRowCnt += rowCount;
        }
      }
    }
    return std::make_unique<RecordCountResult>(affectedRowCnt);
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

std::string SwapQuery::toString()  noexcept {
  return "QUERY = SWAP " + this->targetTable + "\"";
}
