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

static ModuleSQL *me; // TODO: Made proper singalton

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
// ModuleSQL
//------------------------------------------------------------------------------
ModuleSQL::ModuleSQL(const Anope::string &modname, const Anope::string &creator) : Module(modname, creator, EXTRA | VENDOR)
{
  me = this;

  DThread = new DispatcherThread();
  DThread->Start();
}

//------------------------------------------------------------------------------
ModuleSQL::~ModuleSQL()
{
  for (std::map<Anope::string, PgSQLService *>::iterator it = this->PgSQLServices.begin(); it != this->PgSQLServices.end(); ++it)
    delete it->second;
  PgSQLServices.clear();

  DThread->SetExitState();
  DThread->Wakeup();
  DThread->Join();
  delete DThread;
}

//------------------------------------------------------------------------------
void ModuleSQL::OnReload(Configuration::Conf *conf) anope_override
{
  Configuration::Block *config = conf->GetModule(this);

  for (std::map<Anope::string, PgSQLService *>::iterator it = this->PgSQLServices.begin(); it != this->PgSQLServices.end();)
  {
    const Anope::string &cname = it->first;
    PgSQLService *s = it->second;
    int i;

    ++it;

    for (i = 0; i < config->CountBlock("pgsql"); ++i)
      if (config->GetBlock("pgsql", i)->Get<const Anope::string>("name", "pgsql/main") == cname)
        break;

    if (i == config->CountBlock("pgsql"))
    {
      Log(LOG_NORMAL, "pgsql") << "PgSQL: Removing server connection " << cname;

      delete s;
      this->PgSQLServices.erase(cname);
    }
  }

  for (int i = 0; i < config->CountBlock("pgsql"); ++i)
  {
    Configuration::Block *block = config->GetBlock("pgsql", i);
    const Anope::string &connname = block->Get<const Anope::string>("name", "pgsql/main");

    if (this->PgSQLServices.find(connname) == this->PgSQLServices.end())
    {
      const Anope::string &database = block->Get<const Anope::string>("database", "anope");
      const Anope::string &server = block->Get<const Anope::string>("server", "127.0.0.1");
      const Anope::string &user = block->Get<const Anope::string>("username", "anope");
      const Anope::string &password = block->Get<const Anope::string>("password");
      int port = block->Get<int>("port", "5432");

      try
      {
        PgSQLService *ss = new PgSQLService(this, connname, database, server, user, password, port);
        this->PgSQLServices.insert(std::make_pair(connname, ss));

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
void ModuleSQL::OnModuleUnload(User *, Module *m) anope_override
{
  this->DThread->Lock();

  for (unsigned i = this->QueryRequests.size(); i > 0; --i)
  {
    QueryRequest &r = this->QueryRequests[i - 1];

    if (r.sqlinterface && r.sqlinterface->owner == m)
    {
      if (i == 1)
      {
        r.service->Lock.Lock();
        r.service->Lock.Unlock();
      }

      this->QueryRequests.erase(this->QueryRequests.begin() + i - 1);
    }
  }

  this->DThread->Unlock();

  this->OnNotify();
}

//------------------------------------------------------------------------------
void ModuleSQL::OnNotify() anope_override
{
  this->DThread->Lock();
  std::deque<QueryResult> finishedRequests = this->FinishedRequests;
  this->FinishedRequests.clear();
  this->DThread->Unlock();

  for (std::deque<QueryResult>::const_iterator it = finishedRequests.begin(), it_end = finishedRequests.end(); it != it_end; ++it)
  {
    const QueryResult &qr = *it;

    if (!qr.sqlinterface)
      throw SQL::Exception("NULL qr.sqlinterface in PgSQLPipe::OnNotify() ?");

    if (qr.result.GetError().empty())
      qr.sqlinterface->OnResult(qr.result);
    else
      qr.sqlinterface->OnError(qr.result);
  }
}

//------------------------------------------------------------------------------
// DispatcherThread
//------------------------------------------------------------------------------
void DispatcherThread::Run()
{
  this->Lock();

  while (!this->GetExitState())
  {
    if (!me->QueryRequests.empty())
    {
      QueryRequest &r = me->QueryRequests.front();
      this->Unlock();

      Result sresult = r.service->RunQuery(r.query);

      this->Lock();
      if (!me->QueryRequests.empty() && me->QueryRequests.front().query == r.query)
      {
        if (r.sqlinterface)
          me->FinishedRequests.push_back(QueryResult(r.sqlinterface, sresult));
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
PgSQLService::PgSQLService(Module *o, const Anope::string &n, const Anope::string &d, const Anope::string &s, const Anope::string &u, const Anope::string &p, int po)
: Provider(o, n), database(d), server(s), user(u), password(p), port(po), sql(NULL)
{
  Connect();
}

//------------------------------------------------------------------------------
PgSQLService::~PgSQLService()
{
  me->DThread->Lock();
  this->Lock.Lock();
  PQfinish(this->sql);
  this->sql = NULL;

  for (unsigned i = me->QueryRequests.size(); i > 0; --i)
  {
    QueryRequest &r = me->QueryRequests[i - 1];

    if (r.service == this)
    {
      if (r.sqlinterface)
        r.sqlinterface->OnError(Result(0, r.query, "SQL Interface is going away"));
      me->QueryRequests.erase(me->QueryRequests.begin() + i - 1);
    }
  }
  this->Lock.Unlock();
  me->DThread->Unlock();
}

//------------------------------------------------------------------------------
void PgSQLService::Run(Interface *i, const Query &query)
{
  me->DThread->Lock();
  me->QueryRequests.push_back(QueryRequest(this, i, query));
  me->DThread->Unlock();
  me->DThread->Wakeup();
}

//------------------------------------------------------------------------------
Result PgSQLService::RunQuery(const Query &query)
{
  this->Lock.Lock();
    PGresult *res;
  Anope::string real_query = this->BuildQuery(query);

  if (this->CheckConnection() && !(res = PQexec(this->sql, real_query.c_str())))
  {
    // TODO: emulate this somehow?
    //unsigned int id = pgsql_insert_id(this->sql);
    int id = 42;

    this->Lock.Unlock();
    return PgSQLResult(id, query, real_query, res);
  }
  else
  {
    Anope::string error = PQerrorMessage(this->sql);
    this->Lock.Unlock();
    return PgSQLResult(query, real_query, error);
  }
}

//------------------------------------------------------------------------------
std::vector<Query> PgSQLService::CreateTable(const Anope::string &table, const Data &data)
{
  std::vector<Query> queries;
  std::set<Anope::string> &known_cols = this->active_schema[table];

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
  const std::set<Anope::string> &known_cols = this->active_schema[table];
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
  // "-c connect_timeout=5"
  this->sql = PQsetdbLogin(this->server.c_str(), "5432", NULL, NULL, this->database.c_str(), this->user.c_str(), this->password.c_str());

  //const unsigned int timeout = 1;

  //bool connect = pgsql_real_connect(this->sql, this->server.c_str(), this->user.c_str(), this->password.c_str(), this->database.c_str(), this->port, NULL, CLIENT_MULTI_RESULTS);

  if (!this->sql)
    throw SQL::Exception("Unable to connect to PgSQL service " + this->name + ": " + PQerrorMessage(this->sql));

  if(PQstatus(this->sql) == CONNECTION_BAD)
    throw SQL::Exception("Unable to connect to PgSQL service " + this->name + ": " + PQerrorMessage(this->sql));

  Log(LOG_DEBUG) << "Successfully connected to PgSQL service " << this->name << " at " << this->server << ":" << this->port;
}

//------------------------------------------------------------------------------
bool PgSQLService::CheckConnection()
{
  if (!this->sql || (PQstatus(this->sql) == CONNECTION_BAD))
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
Anope::string PgSQLService::Escape(const Anope::string &query)
{
  std::vector<char> buffer(query.length() * 2 + 1);
    int error;
    size_t escapedsize = PQescapeStringConn(this->sql, &buffer[0], query.c_str(), query.length(), &error);
  return &buffer[0];
}

//------------------------------------------------------------------------------
Anope::string PgSQLService::BuildQuery(const Query &q)
{
  Anope::string real_query = q.query;

  for (std::map<Anope::string, QueryData>::const_iterator it = q.parameters.begin(), it_end = q.parameters.end(); it != it_end; ++it)
    real_query = real_query.replace_all_cs("@" + it->first + "@", (it->second.escape ? ("'" + this->Escape(it->second.data) + "'") : it->second.data));

  return real_query;
}

//------------------------------------------------------------------------------
Anope::string PgSQLService::FromUnixtime(time_t t)
{
  return "FROM_UNIXTIME(" + stringify(t) + ")";
}

//------------------------------------------------------------------------------
//MODULE_INIT(ModuleSQL)
