//
// Created by wznmickey on 20221027.
//

#include "DuplicateQuery.h"

#include "../../db/Database.h"

#include <iostream>

constexpr const char *DuplicateQuery::qname;

QueryResult::Ptr DuplicateQuery::execute(int threadsNum) noexcept{
  using namespace std;
  Database &db = Database::getInstance();
  try {
    auto &table = db[this->targetTable];
    auto result = initCondition(table);
    size_t count =0;
    if (result.second) {
      Table::DuplicateQueue queue(&table);
      for (auto it = table.begin(); it != table.end(); ++it) {
        if (this->evalCondition(*it)) {
          queue.push(it);
        }
      }
      count = queue.doDuplicate();
      // std::cout<<"Affected "<<queue.doDuplicate()<<" rows."<<std::endl;

    } else {
      cout << "Error in Duplicate.cpp result.second\n";
    }
    return make_unique<RecordCountResult>(count);
  } catch (const TableNameNotFound &e) {
    return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                       "No such table."s);
  } catch (const IllFormedQueryCondition &e) {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  } catch (const invalid_argument &e) {
    // Cannot convert operand to string
    return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                       "Unknown error '?'"_f % e.what());
  } catch (const exception &e) {
    return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                       "Unkonwn error '?'."_f % e.what());
  }
}

std::string DuplicateQuery::toString() noexcept{
  return "QUERY = DUPLICATE " + this->targetTable + "\"";
}
