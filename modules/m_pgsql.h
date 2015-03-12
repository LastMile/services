//==============================================================================
// File:	m_pgsql.h
// Purpose: Provide interface to postgresql
//==============================================================================
#pragma once

#include "module.h"
#include "modules/sql.h"

#include <cstdlib>
#include <sstream>
#include <libpq-fe.h>

using namespace SQL;

struct QueryRequest;
struct QueryResult;

class PgSQLModule;
class PgSQLResult;
class PgSQLConnection;

//------------------------------------------------------------------------------
// PgSQLModule
//------------------------------------------------------------------------------
class PgSQLModule : public Module, public Pipe
{
  std::map<Anope::string, PgSQLConnection*> m_connections;
  
  public:
  
  std::deque<QueryRequest> QueryRequests;   // Pending query requests
  std::deque<QueryResult> FinishedRequests; // Pending finished requests with results

  PgSQLModule(const Anope::string& _name, const Anope::string& _creator);
  ~PgSQLModule();

  void OnReload(Configuration::Conf* _pConfig) anope_override;
  void OnModuleUnload(User* _pUser, Module* _pModule) anope_override;
  void OnNotify() anope_override;
};

//------------------------------------------------------------------------------
// QueryRequest
//------------------------------------------------------------------------------
struct QueryRequest
{
  PgSQLConnection* pConnection;
  Interface* pSQLInterface; // The interface to use once we have the result to send the data back
  Query query; // The actual query

  QueryRequest(PgSQLConnection* _pConnection, Interface* _pInterface, const Query& _query);
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
// PgSQLConnection
//------------------------------------------------------------------------------
class PgSQLConnection : public Provider
{
  std::map<Anope::string, std::set<Anope::string> > m_activeSchema;

  Anope::string m_username;
  Anope::string m_password;
  Anope::string m_hostname;
  Anope::string m_port;
  Anope::string m_database;
  Anope::string m_schema;

  PGconn* m_pConnection;

  Anope::string Escape(const Anope::string &query);

 public:
  PgSQLConnection(Module* _pO, const Anope::string& _name, const Anope::string& _database, const Anope::string& _hostname, const Anope::string& _username, const Anope::string& _password, const Anope::string& _port);
  ~PgSQLConnection();

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
