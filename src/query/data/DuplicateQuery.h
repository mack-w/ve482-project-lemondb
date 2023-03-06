//
// Created by wznmickey on 20221027
//

#ifndef PROJECT_DUPLICATEQUERY_H
#define PROJECT_DUPLICATEQUERY_H

#include "../Query.h"

class DuplicateQuery : public ComplexQuery {
  static constexpr const char *qname = "DUPLICATE";
private:
  static void threads(DuplicateQuery* th,int total, int index,Table* table,Table::DuplicateQueue *queue,Table::Iterator &end )  noexcept ;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute(int threadsNum) noexcept override;

  std::string toString() noexcept override;

  std::string name() noexcept override { return "DUPLICATE"; }
};

#endif // PROJECT_DUPLICATEQUERY_H