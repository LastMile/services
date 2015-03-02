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
class PgSQLModule;

class PgSQLResult;
class PgSQLService;

//------------------------------------------------------------------------------
// QueryRequest
//------------------------------------------------------------------------------
struct QueryRequest
{
  PgSQLService* pService; // The connection to the database
  Interface* pSQLInterface; // The interface to use once we have the result to send the data back
  Query query; // The actual query

  QueryRequest(PgSQLService* _pService, Interface* _pInterface, const Query& _query);
};

//------------------------------------------------------------------------------
// QueryResult
//------------------------------------------------------------------------------
struct QueryResult
{
  Interface* pSQLInterface;
  Result result;

  QueryResult(Interface* _pInterface, Result& _result);
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
// PgSQLModule
//------------------------------------------------------------------------------
class PgSQLModule : public Module, public Pipe
{
  std::map<Anope::string, PgSQLService*> m_databases;
 public:
  std::deque<QueryRequest> QueryRequests;   // Pending query requests
  std::deque<QueryResult> FinishedRequests; // Pending finished requests with results
  DispatcherThread *DThread;                // The thread used to execute queries

  PgSQLModule(const Anope::string& _name, const Anope::string& _creator);
  ~PgSQLModule();

  void OnReload(Configuration::Conf* _pConfig) anope_override;
  void OnModuleUnload(User* _pUser, Module* _pModule) anope_override;
  void OnNotify() anope_override;
};

//------------------------------------------------------------------------------
// DispatcherThread
//------------------------------------------------------------------------------
class DispatcherThread : public Thread, public Condition
{
  public:
  DispatcherThread();
  ~DispatcherThread();

  void Run() anope_override;
};

//------------------------------------------------------------------------------
// PgSQLService
//------------------------------------------------------------------------------
class PgSQLService : public Provider
{
  std::map<Anope::string, std::set<Anope::string> > m_activeSchema;

  Anope::string m_username;
  Anope::string m_password;
  Anope::string m_hostname;
  Anope::string m_port;
  Anope::string m_database;

  PGconn* m_pConnection;

  /** Escape a query.
   * Note the mutex must be held!
   */
  Anope::string Escape(const Anope::string &query);

 public:
  /* Locked by the SQL thread when a query is pending on this m_database,
   * prevents us from deleting a connection while a query is executing
   * in the thread
   */
  Mutex Lock; // TODO: Move to private

  PgSQLService(Module* _pO, const Anope::string& _name, const Anope::string& _database, const Anope::string& _hostname, const Anope::string& _username, const Anope::string& _password, const Anope::string& _port);
  ~PgSQLService();

  void Run(Interface* _pInterface, const Query& _query) anope_override;
  Result RunQuery(const Query& _rawQuery) anope_override;
  std::vector<Query> CreateTable(const Anope::string& _table, const Data& _data) anope_override;
  Query BuildInsert(const Anope::string& _table, unsigned int _id, Data& _data) anope_override;
  Query GetTables(const Anope::string& _prefix) anope_override;

  void Connect();
  bool CheckConnection();
  Anope::string BuildQuery(const Query& _rawQuery);
  Anope::string FromUnixtime(time_t);
};

//------------------------------------------------------------------------------
MODULE_INIT(PgSQLModule)
