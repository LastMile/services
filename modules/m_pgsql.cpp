/* RequiredLibraries: pq */
/* RequiredWindowsLibraries: libpq */
//==============================================================================
// File:	m_pgsql.cpp
// Purpose: Provide interface to postgresql
//==============================================================================
#include "m_pgsql.h"

static PgSQLModule *me; // TODO: Made proper singalton

//------------------------------------------------------------------------------
// QueryRequest
//------------------------------------------------------------------------------
QueryRequest::QueryRequest(PgSQLConnection* _pService, Interface* _pInterface, const Query& _query)
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
}

//------------------------------------------------------------------------------
PgSQLModule::~PgSQLModule()
{
  // Disconnect
  for (std::map<Anope::string, PgSQLConnection*>::iterator hCurrentConnection = this->m_connections.begin(); hCurrentConnection != this->m_connections.end(); ++hCurrentConnection)
  {
    delete hCurrentConnection->second;
    Log(LOG_NORMAL, "pgsql") << "PgSQL: Removing server connection " << hCurrentConnection->first;
  }
  m_connections.clear();
}

//------------------------------------------------------------------------------
void PgSQLModule::OnReload(Configuration::Conf* _pConfig) anope_override
{
  // Disconnect
  for (std::map<Anope::string, PgSQLConnection*>::iterator hCurrentConnection = this->m_connections.begin(); hCurrentConnection != this->m_connections.end(); ++hCurrentConnection)
  {
    delete hCurrentConnection->second;
    Log(LOG_NORMAL, "pgsql") << "PgSQL: Removing server connection " << hCurrentConnection->first;
  }
  m_connections.clear();
  
  // Connect
  Configuration::Block* pBlock = _pConfig->GetModule(this);
  for (int i = 0; i < pBlock->CountBlock("pgsql"); ++i)
  {
    Configuration::Block* pPgSqlBlock = pBlock->GetBlock("pgsql", i);
    const Anope::string& connectionName = block->Get<const Anope::string>("name", "pgsql/main");

    if (this->m_connections.find(connectionName) == this->m_connections.end())
    {
      const Anope::string &user     = block->Get<const Anope::string>("username", "anope");
      const Anope::string &password = block->Get<const Anope::string>("password");
      const Anope::string &server   = block->Get<const Anope::string>("server", "127.0.0.1");
      const Anope::string &port     = block->Get<const Anope::string>("port", "5432");
      const Anope::string &database = block->Get<const Anope::string>("database", "anope");
      const Anope::string &schema   = block->Get<const Anope::string>("schema", "public");
      
      try
      {
        PgSQLConnection* pConnection = new PgSQLConnection(this, connectionName, database, server, user, password, port);
        this->m_connections.insert(std::make_pair(connectionName, pConnection));

        Log(LOG_NORMAL, "pgsql") << "PgSQL: Successfully connected to server " << connectionName << " (" << server << ")";
      }
      catch (const SQL::Exception& exception)
      {
        Log(LOG_NORMAL, "pgsql") << "PgSQL: " << exception.GetReason();
      }
    }
  }
}

//------------------------------------------------------------------------------
void PgSQLModule::OnModuleUnload(User* _pUser, Module* _pModule) anope_override
{
  for (unsigned i = this->QueryRequests.size(); i > 0; --i)
  {
    QueryRequest& request = this->QueryRequests[i - 1];

    if (request.pSQLInterface && request.pSQLInterface->owner == _pModule)
    {
      this->QueryRequests.erase(this->QueryRequests.begin() + i - 1);
    }
  }
  
  this->OnNotify();
}

//------------------------------------------------------------------------------
void PgSQLModule::OnNotify() anope_override
{
  std::deque<QueryResult> finishedRequests = this->FinishedRequests;
  this->FinishedRequests.clear();

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
// PgSQLConnection
//------------------------------------------------------------------------------
PgSQLConnection::PgSQLConnection(Module* _pO, const Anope::string& _name, const Anope::string& _database, const Anope::string& _hostname, const Anope::string& _username, const Anope::string& _password, const Anope::string& _port)
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
PgSQLConnection::~PgSQLConnection()
{
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
}

//------------------------------------------------------------------------------
void PgSQLConnection::Run(Interface* _pInterface, const Query& _query)
{
  me->QueryRequests.push_back(QueryRequest(this, _pInterface, _query));
}

//------------------------------------------------------------------------------
Result PgSQLConnection::RunQuery(const Query& _rawQuery)
{
  PGresult* pResult;

  Anope::string renderedQuery = this->BuildQuery(_rawQuery);

  if (this->CheckConnection() && !(pResult = PQexec(m_pConnection, renderedQuery.c_str())))
  {
    // TODO: emulate this somehow?
    //unsigned int id = pgsql_insert_id(m_pConnection);
    int id = 42;

    return PgSQLResult(id, _rawQuery, renderedQuery, pResult);
  }

  Anope::string error = PQerrorMessage(m_pConnection);

  return PgSQLResult(_rawQuery, renderedQuery, error);
}

//------------------------------------------------------------------------------
std::vector<Query> PgSQLConnection::CreateTable(const Anope::string &table, const Data &data)
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
Query PgSQLConnection::BuildInsert(const Anope::string &table, unsigned int id, Data &data)
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
Query PgSQLConnection::GetTables(const Anope::string &prefix)
{
  return Query("SHOW TABLES LIKE '" + prefix + "%';");
}

//------------------------------------------------------------------------------
void PgSQLConnection::Connect()
{
  // TODO: Add Timeout
  // "-c connect_timeout=5"
  m_pConnection = PQsetdbLogin(m_hostname.c_str(), m_port.c_str(), NULL, NULL, m_database.c_str(), m_username.c_str(), m_password.c_str());

  if (!m_pConnection || PQstatus(m_pConnection) == CONNECTION_BAD)
    throw SQL::Exception("Unable to connect to the postgres server " + this->name + ": " + PQerrorMessage(m_pConnection));

  Log(LOG_DEBUG) << "Successfully connected to the postgres server " << this->name << " at " << this->m_hostname << ":" << this->m_port;
}

//------------------------------------------------------------------------------
bool PgSQLConnection::CheckConnection()
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
Anope::string PgSQLConnection::Escape(const Anope::string& _query)
{
  Anope::string escapedQuery;
  escapedQuery.resize(_query.length() * 2 + 1);

  int error;
  PQescapeStringConn(m_pConnection, escapedQuery.c_str(), _query.c_str(), _query.length(), &error);

  return &escapedQuery;
}

//------------------------------------------------------------------------------
Anope::string PgSQLConnection::BuildQuery(const Query& _rawQuery)
{
  Anope::string query = _rawQuery.query;

  for (std::map<Anope::string, QueryData>::const_iterator it = _rawQuery.parameters.begin(), it_end = _rawQuery.parameters.end(); it != it_end; ++it)
    query = query.replace_all_cs("@" + it->first + "@", (it->second.escape ? ("'" + this->Escape(it->second.data) + "'") : it->second.data));

  return query;
}

//------------------------------------------------------------------------------
Anope::string PgSQLConnection::FromUnixtime(time_t _time)
{
  return "FROM_UNIXTIME(" + stringify(_time) + ")";
}
