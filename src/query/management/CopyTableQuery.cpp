//
// Created by YichenWei on 22-10-25.
//

#include "CopyTableQuery.h"

#include "../../db/Database.h"

constexpr const char *CopyTableQuery::qname;

QueryResult::Ptr CopyTableQuery::execute(int threadsNum)  noexcept {
  using namespace std;
  Database &db = Database::getInstance();
  // try {
    db.copyTable(this->targetTable, this->newTable);
    return make_unique<SuccessMsgResult>(qname, targetTable);
  // } catch (const TableNameNotFound &e) {
  //   return make_unique<ErrorMsgResult>(qname, targetTable, "No such table."s);
  // } catch (const exception &e) {
  //   return make_unique<ErrorMsgResult>(qname, e.what());
  // }
}

std::string CopyTableQuery::toString()  noexcept {
  return "QUERY = CopyTable TABLE, newTable = \"" + this->newTable + "\"";
}
