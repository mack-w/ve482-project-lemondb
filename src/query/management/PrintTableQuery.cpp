//
// Created by liu on 18-10-25.
//

#include "PrintTableQuery.h"

#include <iostream>

#include "../../db/Database.h"

constexpr const char *PrintTableQuery::qname;

QueryResult::Ptr PrintTableQuery::execute(int threadsNum) noexcept  {
  using namespace std;
  Database &db = Database::getInstance();
  // try {
    auto &table = db[this->targetTable];
    cout << "================\n";
    cout << "TABLE = ";
    cout << table;
    cout << "================\n" << endl;
    return make_unique<SuccessMsgResult>(qname, this->targetTable);
  // } catch (const TableNameNotFound &e) {
  //   return make_unique<ErrorMsgResult>(qname, this->targetTable,
  //                                      "No such table."s);
  // }
}

std::string PrintTableQuery::toString() noexcept  {
  return "QUERY = SHOWTABLE, Table = \"" + this->targetTable + "\"";
}