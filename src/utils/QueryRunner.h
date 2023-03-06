#ifndef QUERY_RUNNER_H
#define QUERY_RUNNER_H
#include <iostream>
#include <thread>
#include <atomic>
#include <mutex>
#include <unistd.h>
#include <fstream>
#include "../query/QueryBuilders.h"
#include "../query/QueryParser.h"
#include <atomic>
#include <iostream>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <future>
enum QueryType {
  QUERY_READ,
  QUERY_WRITE,
  QUERY_LOAD,
  QUERY_COPYTABLE,
  QUERY_EXIT
};

enum QueryStatus { NOT_RUN, RUNNING, FINISHED_WAIT_OUTPUT, FINISHED_OUTPUT };

enum TableStatus { VACANT, READ, WRITE };

struct QueryInfo {
public:
  int readTableNum, writeTableNum;
  QueryType type;
  QueryStatus status;
  Query::Ptr query;
  QueryResult::Ptr result;
  bool ifDone;

  QueryInfo() {}

  void init(int _readTableNum, int _writeTableNum, QueryType _type) {
    this->writeTableNum = _writeTableNum;
    this->readTableNum = _readTableNum;
    this->type = _type;
    this->status = NOT_RUN;
    ifDone = false;
  }
};

struct TableInfo {
public:
  TableStatus tableStatus;
  bool tableLackWrite;
  int tableReadSum;

  TableInfo() {
    tableStatus = VACANT;
    tableLackWrite = false;
    tableReadSum = 0;
  }
};

struct ThreadInfo {
public:
  int threadsUsed;
  int lineNumber;
  std::future<int> x;
  ThreadInfo(int _threadsUsed, int _lineNumber,std::future<int> _x) {
    this->threadsUsed = _threadsUsed;
    this->lineNumber = _lineNumber;
    this->x =std::move(_x);
  }
};

int runQueryThread(int lineNum, int threadsNum);

void startQuery(int lineNum);
void finishQuery(int lineNum);
void outputQuery(int lineNum);

std::thread runQuery(int lineNum);
int findNextQuery(int currLineNum);
void initQueryRunner(std::istream &is, const _parsedArgs &parsedArgs);
void releaseQueryRunner();
void tryOutput();

void queryRunnerLoop();

#endif
