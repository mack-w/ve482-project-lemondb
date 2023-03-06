//
// Created by Jerry on 2022/11/4.
//

#ifndef PROJECT_MAXQUERY_H
#define PROJECT_MAXQUERY_H

#include "../Query.h"
#include <mutex>

class MaxQuery : public ComplexQuery {
  static constexpr const char *qname = "MAX";
private :
  // slice for each thread
  std::mutex result_mtx;

  static void threads(MaxQuery *th,int total, int index, Table* table, std::vector<  int > &maxResults, std::vector<bool>::reference & firstVal) noexcept;
public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute(int threadsNum) noexcept override;

  std::string toString() noexcept override;

  std::string name() noexcept override { return "MAX"; }
};

#endif // PROJECT_MAXQUERY_H
