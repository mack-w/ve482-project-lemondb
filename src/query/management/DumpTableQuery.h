//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_DUMPTABLEQUERY_H
#define PROJECT_DUMPTABLEQUERY_H

#include "../Query.h"

class DumpTableQuery : public Query {
  static constexpr const char *qname = "DUMP";
  const std::string fileName;

public:
  DumpTableQuery(std::string table, std::string filename)
      : Query(std::move(table)), fileName(std::move(filename)) {}

  QueryResult::Ptr execute(int threadsNum)  noexcept  override;

  std::string toString()  noexcept override;

  std::string name() noexcept  override { return "DUMP"; }
};

#endif // PROJECT_DUMPTABLEQUERY_H
