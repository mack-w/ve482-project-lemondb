#include "QueryRunner.h"
#include "threadPool.h"
#include <iostream>
ThreadPool threadPool;

std::vector<QueryInfo> queryInfoArr;
int currQueryLine = 0;
int totLineNum = 0;

std::vector<TableInfo> tableInfoArr;
int totTableNum = 0;
bool ifLoad = 0;

std::vector<ThreadInfo> threadInfoArr;
int threadsMaximum;
int threadsLeft = 0;
int outputLine = 0;

std::mutex ifDoneMutex;
#define QUERY_THREAD_NUM 1
int runQueryThread(int lineNum, int threadsNum) {
  // std::cerr<<"A "<<lineNum<<" "<<threadsNum<<endl;
  queryInfoArr[lineNum].result =
      queryInfoArr[lineNum].query->execute(threadsNum);
  ifDoneMutex.lock();
  queryInfoArr[lineNum].ifDone = true;
  ifDoneMutex.unlock();
  return 0;
}

void startQuery(int lineNum) {
  // std::cerr<<"B "<<lineNum<<endl;
  queryInfoArr[lineNum].status = RUNNING;
  int readTableNum = queryInfoArr[lineNum].readTableNum,
      writeTableNum = queryInfoArr[lineNum].writeTableNum;
  if ((readTableNum == -1) && (writeTableNum == -1))
    return;
  // std::cerr<<"C "<<readTableNum<< " " << writeTableNum<<endl;
  switch (queryInfoArr[lineNum].type) {
  case QUERY_READ:
    tableInfoArr[readTableNum].tableStatus = READ;
    tableInfoArr[readTableNum].tableReadSum++;
    break;

  case QUERY_WRITE:
    tableInfoArr[writeTableNum].tableStatus = WRITE;
    break;

  case QUERY_LOAD:
    tableInfoArr[writeTableNum].tableStatus = WRITE;
    ifLoad = 1;
    break;

  case QUERY_COPYTABLE:
    tableInfoArr[readTableNum].tableStatus = READ;
    tableInfoArr[readTableNum].tableReadSum++;
    tableInfoArr[writeTableNum].tableStatus = WRITE;
    break;

  case QUERY_EXIT:
    tableInfoArr[writeTableNum].tableStatus = WRITE;
    break;

  default:
    break;
  }
}

void finishQuery(int lineNum) {
  // std::cerr << "D: "<<lineNum<<endl;
  queryInfoArr[lineNum].status = FINISHED_WAIT_OUTPUT;
  int readTableNum = queryInfoArr[lineNum].readTableNum,
      writeTableNum = queryInfoArr[lineNum].writeTableNum;
  // std::cerr << "E: "<<readTableNum<<writeTableNum<<endl;
  switch (queryInfoArr[lineNum].type) {
  case QUERY_READ:
    tableInfoArr[readTableNum].tableReadSum--;
    if (tableInfoArr[readTableNum].tableReadSum == 0)
      tableInfoArr[readTableNum].tableStatus = VACANT;
    break;

  case QUERY_WRITE:
    tableInfoArr[writeTableNum].tableStatus = VACANT;
    break;

  case QUERY_LOAD:
    tableInfoArr[writeTableNum].tableStatus = VACANT;
    ifLoad = 0;
    break;

  case QUERY_COPYTABLE:
    tableInfoArr[readTableNum].tableReadSum--;
    if (tableInfoArr[readTableNum].tableReadSum == 0)
      tableInfoArr[readTableNum].tableStatus = VACANT;
    tableInfoArr[writeTableNum].tableStatus = VACANT;

  case QUERY_EXIT:
    break;

  default:
    break;
  }
}

#define NDEBUG
void outputQuery(int lineNum) {
  // std::cerr<<"F: "<<lineNum<<endl;
  if (lineNum != totLineNum - 1) {
    std::cout << lineNum + 1 << "\n";
    if (queryInfoArr[lineNum].result.get()->success()) {
      if (queryInfoArr[lineNum].result.get()->display()) {
        std::cout << *(queryInfoArr[lineNum].result.get());
      } else {
#ifndef NDEBUG
        std::cout.flush();
        std::cerr << *(queryInfoArr[lineNum).result);
#endif
      }
    } else {
      std::cout.flush();
      std::cerr << "QUERY FAILED:\n\t" << *(queryInfoArr[lineNum].result.get());
    }
  }
}

std::future<int> runQuery(int lineNum, int threadsNum) {
  // std::cerr<<"G: "<<lineNum<<threadsNum<<endl;
  startQuery(lineNum);
  return threadPool.enqueueTask(runQueryThread, lineNum, threadsNum);
}

int findNextQueryHelper(int currLineNum) {
  // std::cerr<<"H: "<<currLineNum<<endl;
  for (int i = currLineNum;; i++) {
    if (i == totLineNum - 1) {
      if (queryInfoArr[i].type == QUERY_EXIT)
        if (outputLine == i && queryInfoArr[i].status == NOT_RUN)
          return i;
        else
          return -1;
      else {
        std::cerr << "ERROR in findNextQuery, Last line not QUIT\n";
        return i;
      }
    }
    if (queryInfoArr[i].status != NOT_RUN)
      continue;
    int readTableNum = queryInfoArr[i].readTableNum,
        writeTableNum = queryInfoArr[i].writeTableNum;
    // std::cerr<<"aa"<<readTableNum<<writeTableNum<<endl;
    switch (queryInfoArr[i].type) {
    case QUERY_READ:
      if (ifLoad == 0 &&
          (tableInfoArr[readTableNum].tableStatus == VACANT ||
           tableInfoArr[readTableNum].tableStatus == READ) &&
          tableInfoArr[readTableNum].tableLackWrite == 0)
        return i;
      continue;
      break;

    case QUERY_WRITE:
      if (ifLoad == 0 && tableInfoArr[writeTableNum].tableStatus == VACANT &&
          tableInfoArr[writeTableNum].tableLackWrite == 0)
        return i;
      tableInfoArr[writeTableNum].tableLackWrite = 1;
      continue;

    case QUERY_LOAD:
      if (ifLoad == 0 && tableInfoArr[writeTableNum].tableStatus == VACANT &&
          tableInfoArr[writeTableNum].tableLackWrite == 0)
        return i;
      tableInfoArr[writeTableNum].tableLackWrite = 1;
      continue;

    case QUERY_COPYTABLE:
      if (ifLoad == 0 &&
          (tableInfoArr[readTableNum].tableStatus == VACANT ||
           tableInfoArr[readTableNum].tableStatus == READ) &&
          tableInfoArr[writeTableNum].tableStatus == VACANT &&
          tableInfoArr[readTableNum].tableLackWrite == 0 &&
          tableInfoArr[writeTableNum].tableLackWrite == 0)
        return i;
      tableInfoArr[writeTableNum].tableLackWrite = 1;
      tableInfoArr[readTableNum].tableLackWrite = 1;
      continue;

    default:
      return -1;
      break;
    }
  }
}

int findNextQuery(int currLineNum) {
  // If all Query is finished and final is Quit
  for (int i = 0; i < totTableNum; ++i)
    tableInfoArr[i].tableLackWrite = 0;
  return findNextQueryHelper(currLineNum);
}

std::string extractQueryString(std::istream &is);

int getTableNum(std::unordered_map<std::string, int> &tableNum,
                const std::string &tableName) {
  auto it = tableNum.find(tableName);
  if (it == tableNum.end()) {
    int index = (int)tableNum.size();
    tableNum[tableName] = index;
    return index;
  } else
    return (*it).second;
}

std::vector<std::string> globalFileNameArr;

void initQueryString(std::vector<std::string> &queriesRawString,
                     std::istream &is) {
  std::vector<std::string> fileNameArr;
  while (true) {
    try {
      // std::cerr << "reading\n";
      std::string queryStr = extractQueryString(is);
      queriesRawString.push_back(queryStr);
      if (queryStr.find("LISTEN") != std::string::npos) {
        std::size_t start = queryStr.find('(') + 2;
        std::size_t end = queryStr.find(')') - 1;
        std::string fileName = queryStr.substr(start, end - start);
        std::cerr << "~" << fileName << "~\n";
        fileNameArr.push_back(fileName);
        globalFileNameArr.push_back(fileName);
      }
    } catch (const std::ios_base::failure &e) {
      // End of input
      if (fileNameArr.size() == 0)
        break;
      else {
        for (auto it = fileNameArr.begin(); it != fileNameArr.end(); ++it) {
          std::ifstream fin;
          fin.open(*it);
          std::istream newIs(fin.rdbuf());
          initQueryString(queriesRawString, newIs);
        }
        break;
      }
    }
  }
}

void initQueryRunner(std::istream &is, const _parsedArgs &parsedArgs) {
  std::vector<std::string> queriesRawString;

  if (threadPool.initPool(parsedArgs.threads) == false) {
    std::cerr << "lemondb: critical error: can not initialize thread pool"
              << std::endl;
    exit(1);
  }

  initQueryString(queriesRawString, is);
  totLineNum = (int)queriesRawString.size();

  std::unordered_map<std::string, int> tableNum;
  QueryParser p;
  p.registerQueryBuilder(std::make_unique<QueryBuilder(Debug)>());
  p.registerQueryBuilder(std::make_unique<QueryBuilder(ManageTable)>());
  p.registerQueryBuilder(std::make_unique<QueryBuilder(Complex)>());
  queryInfoArr.reserve(totLineNum);
  auto fileNameArrIt = globalFileNameArr.begin();
  for (int i = 0; i < totLineNum; ++i) {
    if (queriesRawString[i].find("LISTEN") != std::string::npos) {
      queryInfoArr[i].init(-1, -1, QUERY_EXIT);
      queryInfoArr[i].ifDone = true;
      queryInfoArr[i].status = FINISHED_WAIT_OUTPUT;
      std::string msg = "ANSWER = ( listening from " + (*fileNameArrIt) + " )";
      fileNameArrIt++;
      QueryResult::Ptr result = std::make_unique<SelectMsgResult>(msg);
      queryInfoArr[i].result = std::move(result);
      continue;
    }
    queryInfoArr[i].query = p.parseQuery(queriesRawString[i]);
    std::string qname = queryInfoArr[i].query->name(),
                targetTable = queryInfoArr[i].query->targetTable,
                newTable = queryInfoArr[i].query->newTable;
    if (qname == "SELECT" || qname == "SUM" || qname == "COUNT" ||
        qname == "MIN" || qname == "MAX" || qname == "DUMP") {
      int readTableNum = getTableNum(tableNum, targetTable);
      queryInfoArr[i].init(readTableNum, -1, QUERY_READ);
    } else if (qname == "COPYTABLE") {
      int readTableNum = getTableNum(tableNum, targetTable),
          writeTableNum = getTableNum(tableNum, newTable);
      queryInfoArr[i].init(readTableNum, writeTableNum, QUERY_COPYTABLE);
    } else if (qname == "LOAD") {
      int writeTableNum = getTableNum(tableNum, targetTable);
      queryInfoArr[i].init(-1, writeTableNum, QUERY_LOAD);
    } else if (qname == "DROP" || qname == "TRUNCATE" || qname == "DELETE" ||
               qname == "INSERT" || qname == "UPDATE" || qname == "SWAP" ||
               qname == "DUPLICATE" || qname == "ADD" || qname == "SUB") {
      int writeTableNum = getTableNum(tableNum, targetTable);
      queryInfoArr[i].init(-1, writeTableNum, QUERY_WRITE);
    } else if (qname == "QUIT")
      queryInfoArr[i].init(-1, -1, QUERY_EXIT);
    else {
      std::cerr << "Error in initQueryRunner, qname not recognized " << qname
                << "\n";
    }
  }

  totTableNum = (int)tableNum.size();
  tableInfoArr.reserve(totTableNum);
  tableInfoArr.resize(totTableNum);
  for (int i = 0; i < totTableNum; ++i) {
    tableInfoArr[i].tableStatus = VACANT;
    tableInfoArr[i].tableLackWrite = false;
    tableInfoArr[i].tableReadSum = 0;
  }
  ifLoad = 0;

  threadsMaximum = (int)parsedArgs.threads;
  std::cerr << "parsedArgs.threads " << parsedArgs.threads << ", threadMaximum "
            << threadsMaximum << "\n";
  threadsLeft = threadsMaximum;
}

void releaseQueryRunner() {}

void tryOutput() {
  while (outputLine < totLineNum &&
         queryInfoArr[outputLine].status == FINISHED_WAIT_OUTPUT) {
    outputQuery(outputLine);
    queryInfoArr[outputLine].status = FINISHED_OUTPUT;
    outputLine++;
  }
}

void queryRunnerLoop() {
  std::cerr << "threadsLeft " << threadsLeft << "\n";
  while (outputLine != totLineNum) {
    ifDoneMutex.lock();
    for (int i = 0; i < (int)threadInfoArr.size(); ++i)
      if (queryInfoArr[threadInfoArr[i].lineNumber].ifDone) {
        // std::cerr << "joining thread line = " <<
        // threadInfoArr[i].lineNumber
        // << "\n";
        threadInfoArr[i].x.get();
        threadsLeft += threadInfoArr[i].threadsUsed;
        finishQuery(threadInfoArr[i].lineNumber);
        tryOutput();
        // std::cerr << "outputLine, totLineNum: " << outputLine << " " <<
        // totLineNum << "\n";
        threadInfoArr.erase(threadInfoArr.begin() + i);
        break;
      }
    ifDoneMutex.unlock();
    if (threadsLeft > 0) {
      int nextLine = findNextQuery(currQueryLine);
      if (nextLine != -1) {
        // std::cerr << "currQueryLine = " << currQueryLine << "\n";
        if (nextLine == currQueryLine)
          for (int i = currQueryLine + 1; i < totLineNum; ++i)
            if (queryInfoArr[i].status == NOT_RUN) {
              currQueryLine = i;
              break;
            }

        std::cerr << "runQuery lineNumber = " << nextLine << "\n";
        int numThreads = std::max(1,std::min(threadsLeft/3,8));
        if (queryInfoArr[nextLine].query->name() == "MAX") {
          threadInfoArr.push_back(
              ThreadInfo(numThreads, nextLine, runQuery(nextLine, numThreads)));
          threadsLeft -= numThreads;
        } else if (queryInfoArr[nextLine].query->name() == "DELETE") {
          threadInfoArr.push_back(
              ThreadInfo(numThreads, nextLine, runQuery(nextLine, numThreads)));
          threadsLeft -= numThreads;
        } else if (queryInfoArr[nextLine].query->name() == "COUNT") {
          threadInfoArr.push_back(
              ThreadInfo(numThreads, nextLine, runQuery(nextLine, numThreads)));
          threadsLeft -= numThreads;
        }
         else if (queryInfoArr[nextLine].query->name() == "MIN") {
          threadInfoArr.push_back(
              ThreadInfo(numThreads, nextLine, runQuery(nextLine, numThreads)));
          threadsLeft -= numThreads;
        }else {
          threadInfoArr.push_back(
              ThreadInfo(QUERY_THREAD_NUM, nextLine,
                         runQuery(nextLine, QUERY_THREAD_NUM)));
          threadsLeft -= QUERY_THREAD_NUM;
        }
      }
    }
    // usleep(1);
  }
  std::cerr << "exit queryRunnerLoop\n";
}
