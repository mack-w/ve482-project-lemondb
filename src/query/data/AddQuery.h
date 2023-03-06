//
// Created by Mack 22-11-03
//

#ifndef PROJECT_ADDQUERY_H
#define PROJECT_ADDQUERY_H

#include "../Query.h"

class AddQuery : public ComplexQuery {
  static constexpr const char *qname = "ADD";

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute(int threadsNum) noexcept override;

  std::string toString() noexcept override;

  std::string name() noexcept override { return "ADD"; }
};

#endif // PROJECT_ADDQUERY_H
