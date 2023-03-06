//
// Created by YichenWei on 22-10-24.
//

#include "TruncateTableQuery.h"

#include "../../db/Database.h"

constexpr const char *TruncateTableQuery::qname;

QueryResult::Ptr TruncateTableQuery::execute(int threadsNum)  noexcept {
  using namespace std;
  Database &db = Database::getInstance();
  // try {
    auto &table = db[this->targetTable];
    table.clear();
  // } catch (const TableNameNotFound &e) {
  //   return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
  // }  catch (const exception &e) {
  //   return make_unique<ErrorMsgResult>(qname, e.what());
  // }
  return make_unique<SuccessMsgResult>(qname);
}

std::string TruncateTableQuery::toString()  noexcept {
  return "QUERY = TRUNCATE, Table = \"" + targetTable + "\"";
}