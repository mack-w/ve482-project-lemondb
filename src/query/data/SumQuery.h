//
// Created by Jerry on 2022/11/3.
//

#ifndef LEMONDB_SUMQUERY_H
#define LEMONDB_SUMQUERY_H

#include "../Query.h"

class SumQuery : public ComplexQuery {
  static constexpr const char *qname = "SUM";
private :
  const int slice = 100; // slice for each thread

  void threads(int total, int index, Table* table, std::vector< std::pair<size_t, int> > &sumResults) noexcept;
public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute(int threadsNum) noexcept override;

  std::string toString() noexcept override;

  std::string name() noexcept override { return "SUM"; }
};

#endif // LEMONDB_SUMQUERY_H
