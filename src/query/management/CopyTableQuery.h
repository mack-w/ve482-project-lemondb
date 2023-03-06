//
// Created by YichenWei on 22-10-25.
//

#ifndef PROJECT_COPYTABLEQUERY_H
#define PROJECT_COPYTABLEQUERY_H

#include "../Query.h"
#include <iostream>

class CopyTableQuery : public Query {
  static constexpr const char *qname = "COPYTABLE";

public:
  CopyTableQuery(std::string table, std::string newTable)
      : Query(std::move(table)) { this->newTable = newTable; }

  QueryResult::Ptr execute(int threadsNum)  noexcept override;

  std::string toString()  noexcept override;

  std::string name()  noexcept override { return "COPYTABLE"; }
};

#endif // PROJECT_DUMPTABLEQUERY_H
