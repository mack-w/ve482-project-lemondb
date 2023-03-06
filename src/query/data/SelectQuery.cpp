//
// Created by YichenWei on 22-10-23.
//

#include "SelectQuery.h"

#include <iostream>
#include <algorithm>

#include "../../db/Database.h"
#include "../../db/Table.h"

constexpr const char *SelectQuery::qname;

bool compareKeys(const std::pair<std::string, std::string> &e1, const std::pair<std::string, std::string> &e2){
  return (e1.first < e2.first);
}

QueryResult::Ptr SelectQuery::execute(int threadsNum)  noexcept {
  using namespace std;
  if (this->operands[0] != "KEY")
    return make_unique<ErrorMsgResult>(
      qname, this->targetTable.c_str(),
      "Invalid number of operands (? operands) or without leading KEY."_f % operands.size());
  Database &db = Database::getInstance();
  string selectMsg = "";
  // try {
    auto &table = db[this->targetTable];
    vector<size_t> fieldsNeeded;// This stores the fields that need to print
    for (auto it = this->operands.begin() + 1; it != operands.end(); ++ it) //this->operands.begin() + 1 to skip leading KEY
      fieldsNeeded.push_back(table.getFieldIndex(*it));
    auto result = initCondition(table);
    if (result.second) {
      vector<pair<string, string>> outputLists; //first item: key, second item: following fields
      for (auto it = table.begin(); it != table.end(); ++it) {
        if (this->evalCondition(*it)) {
          string fieldsString = "";
          for (auto fieldIt = fieldsNeeded.begin(); fieldIt != fieldsNeeded.end(); ++fieldIt)
            fieldsString += " " + to_string((*it)[*fieldIt]);
          outputLists.push_back(make_pair((*it).key(), fieldsString));
        }
      }
      sort(outputLists.begin(), outputLists.end(), compareKeys);
      for (auto it = outputLists.begin(); it != outputLists.end(); ++it)
        if (it == outputLists.begin())
          selectMsg += "( " + (*it).first + (*it).second + " )";
        else
          selectMsg += "\n( " + (*it).first + (*it).second + " )";
    }
    else
    {
      cout << "Error in SelectQuery.cpp result.second\n";
    }
    if (selectMsg=="")return make_unique<NullQueryResult>();
    return make_unique<SelectMsgResult>(selectMsg);
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

std::string SelectQuery::toString()  noexcept {
  return "QUERY = SELECT " + this->targetTable + "\"";
}