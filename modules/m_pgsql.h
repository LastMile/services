//==============================================================================
// File:	m_pgsql.h
// Purpose: Provide interface to postgresql
//==============================================================================
#pragma once

#include "module.h"
#include "modules/sql.h"

#include <cstdlib>
#include <sstream>

#define NO_CLIENT_LONG_LONG // TODO:: Whats this for?
#include <libpq-fe.h>

using namespace SQL;

/** Non blocking threaded postgresql API, based on anope's m_mysql.cpp
 *
 * This module spawns a single thread that is used to execute blocking PgSQL queries.
 * When a module requests a query to be executed it is added to a list for the thread
 * (which never stops looping and sleeping) to pick up and execute, the result of which
 * is inserted in to another queue to be picked up by the main thread. The main thread
 * uses Pipe to become notified through the socket engine when there are results waiting
 * to be sent back to the modules requesting the query
 */

struct QueryRequest;
struct QueryResult;

class DispatcherThread;
class ModuleSQL;

class PgSQLResult;
class PgSQLService;

//------------------------------------------------------------------------------
// QueryRequest
//------------------------------------------------------------------------------
struct QueryRequest
{
  /* The connection to the database */
  PgSQLService *service;
  /* The interface to use once we have the result to send the data back */
  Interface *sqlinterface;
  /* The actual query */
  Query query;

  QueryRequest(PgSQLService *s, Interface *i, const Query &q) : service(s), sqlinterface(i), query(q) { }
};

//------------------------------------------------------------------------------
// QueryResult
//------------------------------------------------------------------------------
struct QueryResult
{
  /* The interface to send the data back on */
  Interface *sqlinterface;
  /* The result */
  Result result;

  QueryResult(Interface *i, Result &r) : sqlinterface(i), result(r) { }
};

//------------------------------------------------------------------------------
// PgSQLResult
//------------------------------------------------------------------------------
class PgSQLResult : public Result
{
  PGresult* m_pResult;

 public:
  PgSQLResult(unsigned int _id, const Query& _rawQuery, const Anope::string& _renderedQuery, PGresult* _pResult);
  PgSQLResult(const Query& _rawQuery, const Anope::string& _renderedQuery, const Anope::string& _error);
  ~PgSQLResult();
};

//------------------------------------------------------------------------------
// ModuleSQL
//------------------------------------------------------------------------------
class ModuleSQL : public Module, public Pipe
{
  /* SQL connections */
  std::map<Anope::string, PgSQLService *> PgSQLServices;
 public:
  /* Pending query requests */
  std::deque<QueryRequest> QueryRequests;
  /* Pending finished requests with results */
  std::deque<QueryResult> FinishedRequests;
  /* The thread used to execute queries */
  DispatcherThread *DThread;

  ModuleSQL(const Anope::string &modname, const Anope::string &creator);

  ~ModuleSQL();

  void OnReload(Configuration::Conf *conf) anope_override;

  void OnModuleUnload(User *, Module *m) anope_override;

  void OnNotify() anope_override;
};

//------------------------------------------------------------------------------
// DispatcherThread
//------------------------------------------------------------------------------
class DispatcherThread : public Thread, public Condition
{
  public:

  DispatcherThread() : Thread() { }
  void Run() anope_override;
};

//------------------------------------------------------------------------------
// PgSQLService
//------------------------------------------------------------------------------
class PgSQLService : public Provider
{
  std::map<Anope::string, std::set<Anope::string> > active_schema;

  Anope::string database;
  Anope::string server;
  Anope::string user;
  Anope::string password;
  int port;

  PGconn *sql;

  /** Escape a query.
   * Note the mutex must be held!
   */
  Anope::string Escape(const Anope::string &query);

 public:
  /* Locked by the SQL thread when a query is pending on this database,
   * prevents us from deleting a connection while a query is executing
   * in the thread
   */
  Mutex Lock; // TODO: Move to private

  PgSQLService(Module *o, const Anope::string &n, const Anope::string &d, const Anope::string &s, const Anope::string &u, const Anope::string &p, int po);

  ~PgSQLService();

  void Run(Interface *i, const Query &query) anope_override;

  Result RunQuery(const Query &query) anope_override;

  std::vector<Query> CreateTable(const Anope::string &table, const Data &data) anope_override;

  Query BuildInsert(const Anope::string &table, unsigned int id, Data &data) anope_override;

  Query GetTables(const Anope::string &prefix) anope_override;

  void Connect();

  bool CheckConnection();

  Anope::string BuildQuery(const Query &q);

  Anope::string FromUnixtime(time_t);
};

//------------------------------------------------------------------------------
MODULE_INIT(ModuleSQL)
