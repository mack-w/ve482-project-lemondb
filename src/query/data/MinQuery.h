//
// Created by Jerry on 2022/11/4.
//

#ifndef PROJECT_MINQUERY_H
#define PROJECT_MINQUERY_H

#include "../Query.h"
#include <mutex>
// const int slice = 5; 
class MinQuery : public ComplexQuery {
  static constexpr const char *qname = "MIN";
private :
  // const int slice = 100; // slice for each thread
  std::mutex result_mtx;

  static void threads(MinQuery *th,int total, int index, Table* table, std::vector<  int > &minResults, std::vector<bool>::reference & firstVal) noexcept;
public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute(int threadsNum) noexcept override;

  std::string toString() noexcept override;

  std::string name() noexcept override { return "MIN"; }
};

#endif // PROJECT_MINQUERY_H
