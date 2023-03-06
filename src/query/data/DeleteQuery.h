//
// Created by wznmickey on 20221026.
// 
#ifndef PROJECT_DELETEQUERY_H
#define PROJECT_DELETEQUERY_H

#include "../Query.h"
class DeleteQuery : public ComplexQuery {
  static constexpr const char *qname = "DELETE";
private:
  static void threads(DeleteQuery* th,int total, int index,Table* table,Table::DeleteQueue* queue )  noexcept ;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute(int threadsNum) noexcept override;

  std::string toString() noexcept override;

  std::string name() noexcept override { return "DELETE"; }
};
#endif //PROJECT_DELETEQUERY_H