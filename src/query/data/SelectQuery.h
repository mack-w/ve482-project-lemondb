#ifndef PROJECT_SELECTQUERY_H
#define PROJECT_SELECTQUERY_H

#include "../Query.h"

class SelectQuery : public ComplexQuery {
  static constexpr const char *qname = "SELECT";

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute(int threadsNum)  noexcept  override   ;

  std::string toString()  noexcept   override ;

  std::string name()   noexcept override   { return "SELECT"; }
};

#endif // PROJECT_SELECTQUERY_H
