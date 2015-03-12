/* RequiredLibraries: pq */
/* RequiredWindowsLibraries: libpq */
//==============================================================================
// File:	m_pgsql.cpp
// Purpose: Provide interface to postgresql
//==============================================================================
#include "m_pgsql.h"

/** Non blocking threaded postgresql API, based on anope's m_mysql.cpp
 *
 * This module spawns a single thread that is used to execute blocking PgSQL queries.
 * When a module requests a query to be executed it is added to a list for the thread
 * (which never stops looping and sleeping) to pick up and execute, the result of which
 * is inserted in to another queue to be picked up by the main thread. The main thread
 * uses Pipe to become notified through the socket engine when there are results waiting
 * to be sent back to the modules requesting the query
 */

static PgSQLModule *me; // TODO: Made proper singalton

//------------------------------------------------------------------------------
// QueryRequest
//------------------------------------------------------------------------------
QueryRequest::QueryRequest(PgSQLService* _pService, Interface* _pInterface, const Query& _query)
  : pService(_pService),
    pSQLInterface(_pInterface),
    query(_query)
{

}

//------------------------------------------------------------------------------
// QueryResult
//------------------------------------------------------------------------------
QueryResult::QueryResult(Interface* _pInterface, Result& _result)
  : pSQLInterface(_pInterface),
    result(_result)
{

}

//------------------------------------------------------------------------------
// PgSQLResult
//------------------------------------------------------------------------------
PgSQLResult::PgSQLResult(unsigned int _id, const Query& _rawQuery, const Anope::string& _renderedQuery, PGresult* _pResult)
  : Result(_id, _rawQuery, _renderedQuery),
  m_pResult(_pResult)
{
  int num_fields = m_pResult ? PQnfields(m_pResult) : 0;

  /* It is not thread safe to log anything here using Log(this->owner) now :( */

  if (num_fields == 0)
    return;

  for (int row_num = 0; row_num < PQntuples(m_pResult); row_num++)
  {
      std::map<Anope::string, Anope::string> items;

      for (int field_count = 0; field_count < num_fields; ++field_count)
      {
          Anope::string column = (PQfname(m_pResult, field_count) ? PQfname(m_pResult, field_count) : "");
          Anope::string data = Anope::string(PQgetvalue(m_pResult, row_num, field_count), PQgetlength(m_pResult, row_num, field_count));

          items[column] = data;
      }

      this->entries.push_back(items);
  }
}

//------------------------------------------------------------------------------
PgSQLResult::PgSQLResult(const Query& _rawQuery, const Anope::string& _renderedQuery, const Anope::string& _error)
  : Result(0, _rawQuery, _renderedQuery, _error),
  m_pResult(nullptr)
{
}

//------------------------------------------------------------------------------
PgSQLResult::~PgSQLResult()
{
  if (this->m_pResult)
    PQclear(this->m_pResult);
}

//------------------------------------------------------------------------------
// PgSQLModule
//------------------------------------------------------------------------------
PgSQLModule::PgSQLModule(const Anope::string& _name, const Anope::string& _creator)
  : Module(_name, _creator, EXTRA | VENDOR)
{
  me = this;

  DThread = new DispatcherThread();
  DThread->Start();
}

//------------------------------------------------------------------------------
PgSQLModule::~PgSQLModule()
{
  for (std::map<Anope::string, PgSQLService*>::iterator it = this->m_databases.begin(); it != this->m_databases.end(); ++it)
    delete it->second;

  m_databases.clear();

  DThread->SetExitState();
  DThread->Wakeup();
  DThread->Join();

  delete DThread;
}

//------------------------------------------------------------------------------
void PgSQLModule::OnReload(Configuration::Conf* _pConfig) anope_override
{
  Configuration::Block* pBlock = _pConfig->GetModule(this);

  for (std::map<Anope::string, PgSQLService *>::iterator it = this->m_databases.begin(); it != this->m_databases.end();)
  {
    const Anope::string &cname = it->first;
    PgSQLService *s = it->second;
    int i;

    ++it;

    for (i = 0; i < pBlock->CountBlock("pgsql"); ++i)
      if (pBlock->GetBlock("pgsql", i)->Get<const Anope::string>("name", "pgsql/main") == cname)
        break;

    if (i == pBlock->CountBlock("pgsql"))
    {
      Log(LOG_NORMAL, "pgsql") << "PgSQL: Removing server connection " << cname;

      delete s;
      this->m_databases.erase(cname);
    }
  }

  for (int i = 0; i < pBlock->CountBlock("pgsql"); ++i)
  {
    Configuration::Block *block = pBlock->GetBlock("pgsql", i);
    const Anope::string &connname = block->Get<const Anope::string>("name", "pgsql/main");

    if (this->m_databases.find(connname) == this->m_databases.end())
    {
      const Anope::string &database = block->Get<const Anope::string>("database", "anope");
      const Anope::string &server = block->Get<const Anope::string>("server", "127.0.0.1");
      const Anope::string &user = block->Get<const Anope::string>("username", "anope");
      const Anope::string &password = block->Get<const Anope::string>("password");
      int port = block->Get<int>("port", "5432");

      try
      {
        PgSQLService *ss = new PgSQLService(this, connname, database, server, user, password, port);
        this->m_databases.insert(std::make_pair(connname, ss));

        Log(LOG_NORMAL, "pgsql") << "PgSQL: Successfully connected to server " << connname << " (" << server << ")";
      }
      catch (const SQL::Exception &ex)
      {
        Log(LOG_NORMAL, "pgsql") << "PgSQL: " << ex.GetReason();
      }
    }
  }
}

//------------------------------------------------------------------------------
void PgSQLModule::OnModuleUnload(User* _pUser, Module* _pModule) anope_override
{
  this->DThread->Lock();

  for (unsigned i = this->QueryRequests.size(); i > 0; --i)
  {
    QueryRequest& request = this->QueryRequests[i - 1];

    if (request.pSQLInterface && request.pSQLInterface->owner == _pModule)
    {
      if (i == 1)
      {
        request.pService->Lock.Lock();
        request.pService->Lock.Unlock();
      }

      this->QueryRequests.erase(this->QueryRequests.begin() + i - 1);
    }
  }

  this->DThread->Unlock();
  this->OnNotify();
}

//------------------------------------------------------------------------------
void PgSQLModule::OnNotify() anope_override
{
  this->DThread->Lock();
  std::deque<QueryResult> finishedRequests = this->FinishedRequests;
  this->FinishedRequests.clear();
  this->DThread->Unlock();

  for (std::deque<QueryResult>::const_iterator it = finishedRequests.begin(), it_end = finishedRequests.end(); it != it_end; ++it)
  {
    const QueryResult& queryResult = *it;

    if (!queryResult.pSQLInterface)
      throw SQL::Exception("NULL queryResult.pSQLInterface in PgSQLPipe::OnNotify() ?");

    if (queryResult.result.GetError().empty())
      queryResult.pSQLInterface->OnResult(queryResult.result);
    else
      queryResult.pSQLInterface->OnError(queryResult.result);
  }
}

//------------------------------------------------------------------------------
// DispatcherThread
//------------------------------------------------------------------------------
DispatcherThread::DispatcherThread()
  : Thread()
{

}

//------------------------------------------------------------------------------
DispatcherThread::~DispatcherThread()
{

}

//------------------------------------------------------------------------------
void DispatcherThread::Run() anope_override
{
  this->Lock();

  while (!this->GetExitState())
  {
    if (!me->QueryRequests.empty())
    {
      QueryRequest& request = me->QueryRequests.front();
      this->Unlock();

      Result sresult = request.pService->RunQuery(request.query);

      this->Lock();
      if (!me->QueryRequests.empty() && me->QueryRequests.front().query == request.query)
      {
        if (request.pSQLInterface)
          me->FinishedRequests.push_back(QueryResult(request.pSQLInterface, sresult));

        me->QueryRequests.pop_front();
      }
    }
    else
    {
      if (!me->FinishedRequests.empty())
        me->Notify();
      this->Wait();
    }
  }

  this->Unlock();
}

//------------------------------------------------------------------------------
// PgSQLService
//------------------------------------------------------------------------------
PgSQLService::PgSQLService(Module* _pO, const Anope::string& _name, const Anope::string& _database, const Anope::string& _hostname, const Anope::string& _username, const Anope::string& _password, const Anope::string& _port)
  : Provider(_pO, _name),
  m_username(_username),
  m_password(_password),
  m_hostname(_hostname),
  m_port(_port),
  m_database(_database),
  m_pConnection(nullptr)
{
  Connect();
}

//------------------------------------------------------------------------------
PgSQLService::~PgSQLService()
{
  me->DThread->Lock();
  this->Lock.Lock();
  PQfinish(m_pConnection);
  m_pConnection = NULL;

  for (unsigned i = me->QueryRequests.size(); i > 0; --i)
  {
    QueryRequest& request = me->QueryRequests[i - 1];

    if (request.pService == this)
    {
      if (request.pSQLInterface)
        request.pSQLInterface->OnError(Result(0, request.query, "SQL Interface is going away"));
      me->QueryRequests.erase(me->QueryRequests.begin() + i - 1);
    }
  }
  this->Lock.Unlock();
  me->DThread->Unlock();
}

//------------------------------------------------------------------------------
void PgSQLService::Run(Interface* _pInterface, const Query& _query)
{
  me->DThread->Lock();
  me->QueryRequests.push_back(QueryRequest(this, _pInterface, _query));
  me->DThread->Unlock();
  me->DThread->Wakeup();
}

//------------------------------------------------------------------------------
Result PgSQLService::RunQuery(const Query& _rawQuery)
{
  PGresult* pResult;

  this->Lock.Lock();

  Anope::string renderedQuery = this->BuildQuery(_rawQuery);

  if (this->CheckConnection() && !(pResult = PQexec(m_pConnection, renderedQuery.c_str())))
  {
    // TODO: emulate this somehow?
    //unsigned int id = pgsql_insert_id(m_pConnection);
    int id = 42;

    this->Lock.Unlock();
    return PgSQLResult(id, _rawQuery, renderedQuery, pResult);
  }

  Anope::string error = PQerrorMessage(m_pConnection);

  this->Lock.Unlock();
  return PgSQLResult(_rawQuery, renderedQuery, error);
}

//------------------------------------------------------------------------------
std::vector<Query> PgSQLService::CreateTable(const Anope::string &table, const Data &data)
{
  std::vector<Query> queries;
  std::set<Anope::string> &known_cols = m_activeSchema[table];

  if (known_cols.empty())
  {
    Log(LOG_DEBUG) << "m_pgsql: Fetching columns for " << table;

    Result columns = this->RunQuery("SHOW COLUMNS FROM \"" + table + "\"");
    for (int i = 0; i < columns.Rows(); ++i)
    {
      const Anope::string &column = columns.Get(i, "Field");

      Log(LOG_DEBUG) << "m_pgsql: Column #" << i << " for " << table << ": " << column;
      known_cols.insert(column);
    }
  }

  if (known_cols.empty())
  {
    Anope::string query_text = "CREATE TABLE \"" + table + "\" (\"id\" int(10) unsigned NOT NULL AUTO_INCREMENT,"
      " \"timestamp\" timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
    for (Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
    {
      known_cols.insert(it->first);

      query_text += ", \"" + it->first + "\" ";
      if (data.GetType(it->first) == Serialize::Data::DT_INT)
        query_text += "int(11)";
      else
        query_text += "text";
    }
    query_text += ", PRIMARY KEY (\"id\"), KEY \"timestamp_idx\" (\"timestamp\"))";
    queries.push_back(query_text);
  }
  else
    for (Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
    {
      if (known_cols.count(it->first) > 0)
        continue;

      known_cols.insert(it->first);

      Anope::string query_text = "ALTER TABLE \"" + table + "\" ADD \"" + it->first + "\" ";
      if (data.GetType(it->first) == Serialize::Data::DT_INT)
        query_text += "int(11)";
      else
        query_text += "text";

      queries.push_back(query_text);
    }

  return queries;
}

//------------------------------------------------------------------------------
Query PgSQLService::BuildInsert(const Anope::string &table, unsigned int id, Data &data)
{
  /* Empty columns not present in the data set */
  const std::set<Anope::string> &known_cols = m_activeSchema[table];
  for (std::set<Anope::string>::iterator it = known_cols.begin(), it_end = known_cols.end(); it != it_end; ++it)
    if (*it != "id" && *it != "timestamp" && data.data.count(*it) == 0)
      data[*it] << "";

  Anope::string query_text = "INSERT INTO \"" + table + "\" (\"id\"";
  for (Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
    query_text += ",\"" + it->first + "\"";
  query_text += ") VALUES (" + stringify(id);
  for (Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
    query_text += ",@" + it->first + "@";
  query_text += ") ON DUPLICATE KEY UPDATE ";
  for (Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
    query_text += "\"" + it->first + "\"=VALUES(\"" + it->first + "\"),";
  query_text.erase(query_text.end() - 1);

  Query query(query_text);
  for (Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
  {
    Anope::string buf;
    *it->second >> buf;
    query.SetValue(it->first, buf);
  }

  return query;
}

//------------------------------------------------------------------------------
Query PgSQLService::GetTables(const Anope::string &prefix)
{
  return Query("SHOW TABLES LIKE '" + prefix + "%';");
}

//------------------------------------------------------------------------------
void PgSQLService::Connect()
{
  // TODO: Add Timeout
  // "-c connect_timeout=5"
  m_pConnection = PQsetdbLogin(m_hostname.c_str(), m_port.c_str(), NULL, NULL, m_database.c_str(), m_username.c_str(), m_password.c_str());

  if (!m_pConnection || PQstatus(m_pConnection) == CONNECTION_BAD)
    throw SQL::Exception("Unable to connect to the postgres server " + this->name + ": " + PQerrorMessage(m_pConnection));

  Log(LOG_DEBUG) << "Successfully connected to the postgres server " << this->name << " at " << this->m_hostname << ":" << this->m_port;
}

//------------------------------------------------------------------------------
bool PgSQLService::CheckConnection()
{
  if (!m_pConnection || (PQstatus(m_pConnection) == CONNECTION_BAD))
  {
    try
    {
      this->Connect();
    }
    catch (const SQL::Exception &)
    {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
Anope::string PgSQLService::Escape(const Anope::string& _query)
{
  Anope::string escapedQuery;
  escapedQuery.resize(_query.length() * 2 + 1);

  int error;
  PQescapeStringConn(m_pConnection, escapedQuery.c_str(), _query.c_str(), _query.length(), &error);

  return &escapedQuery;
}

//------------------------------------------------------------------------------
Anope::string PgSQLService::BuildQuery(const Query& _rawQuery)
{
  Anope::string query = _rawQuery.query;

  for (std::map<Anope::string, QueryData>::const_iterator it = _rawQuery.parameters.begin(), it_end = _rawQuery.parameters.end(); it != it_end; ++it)
    query = query.replace_all_cs("@" + it->first + "@", (it->second.escape ? ("'" + this->Escape(it->second.data) + "'") : it->second.data));

  return query;
}

//------------------------------------------------------------------------------
Anope::string PgSQLService::FromUnixtime(time_t _time)
{
  return "FROM_UNIXTIME(" + stringify(_time) + ")";
}
