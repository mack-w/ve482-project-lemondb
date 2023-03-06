//
// Created by wznmickey on 20221028
//

#ifndef PROJECT_COUNTQUERY_H
#define PROJECT_COUNTQUERY_H

#include "../Query.h"

class CountQuery : public ComplexQuery {
  static constexpr const char *qname = "COUNT";
private :
static void threads(CountQuery* th,int total, int index,Table* table,size_t* ans) noexcept; 
public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute(int threadsNum) noexcept override;

  std::string toString() noexcept override;

  std::string name() noexcept override { return "COUNT"; }
};

#endif // PROJECT_COUNTQUERY_H
