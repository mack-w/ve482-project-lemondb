//
// Created by YichenWei on 22-10-24.
//

#ifndef PROJECT_TRUNCATETABLEQUERY_H
#define PROJECT_TRUNCATETABLEQUERY_H

#include "../Query.h"

class TruncateTableQuery : public Query {
  static constexpr const char *qname = "TRUNCATE";

public:
  using Query::Query;

  QueryResult::Ptr execute(int threadsNum)  noexcept override;

  std::string toString()  noexcept override;

  std::string name()  noexcept override { return "TRUNCATE"; }
};

#endif // PROJECT_TRUNCATETABLEQUERY_H