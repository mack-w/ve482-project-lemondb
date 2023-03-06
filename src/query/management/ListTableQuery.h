//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_LISTTABLEQUERY_H
#define PROJECT_LISTTABLEQUERY_H

#include "../Query.h"

class ListTableQuery : public Query {
  static constexpr const char *qname = "LIST";

public:
  QueryResult::Ptr execute(int threadsNum)  noexcept override;

  std::string toString()  noexcept override;

  std::string name()  noexcept override { return "LIST"; }
};

#endif // PROJECT_LISTTABLEQUERY_H
