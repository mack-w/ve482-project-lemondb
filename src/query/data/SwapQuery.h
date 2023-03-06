#ifndef PROJECT_SWAPQUERY_H
#define PROJECT_SWAPQUERY_H

#include "../Query.h"

class SwapQuery : public ComplexQuery {
  static constexpr const char *qname = "SWAP";
private :
  const int slice = 10; // slice for each thread

  void threads(int total, int index, Table* table, size_t op1, size_t op2, size_t &rowCount) noexcept;

public:
  using ComplexQuery::ComplexQuery;

  QueryResult::Ptr execute(int threadsNum) noexcept override;

  std::string toString() noexcept override;

  std::string name() noexcept override { return "SWAP"; }
};

#endif // PROJECT_SWAPQUERY_H
