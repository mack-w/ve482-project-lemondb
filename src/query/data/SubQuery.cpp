//
// Created by Mack on 22-11-05.
//

#include "SubQuery.h"

#include "../../db/Database.h"

constexpr const char *SubQuery::qname;

QueryResult::Ptr SubQuery::execute(int threadsNum)  noexcept {
  using namespace std;
  if (this->operands.size() < 2)
    return make_unique<ErrorMsgResult>(
        qname, this->targetTable.c_str(),
        "Invalid number of operands (? operands)."_f % operands.size());
  Database &db = Database::getInstance();
  Table::SizeType affecRowCnt = 0;
  Table::ValueType sum = 0;
  // try {
    auto &table = db[this->targetTable];
    vector<string> srcFields;
    string destField;
    srcFields.reserve(srcFields.size()+this->operands.size());
    // construct source field
    for (auto &fieldName : this->operands)
      srcFields.push_back(fieldName);
    srcFields.erase(srcFields.end() - 1);
    // destination field
    destField = *(this->operands.end() - 1);
    // reuse much code from AddQuery
    int cnt = 0;
    auto result = initCondition(table);
    if (result.second)
      for (auto datum : table) {
        if (evalCondition(datum)) {
          for (auto &srcFieldName : srcFields) {
            if (cnt == 0)
              sum = datum[srcFieldName];
            else
              sum -= datum[srcFieldName];
            cnt++;
          }
          datum[destField] = sum;
          affecRowCnt++;
        }
        cnt = 0;
      }
    // print debug info
    return std::make_unique<RecordCountResult>(affecRowCnt);
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

std::string SubQuery::toString() noexcept  {
  return "QUERY = SUB " + this->targetTable + "\"";
}
