/* RequiredLibraries: pq */
/* RequiredWindowsLibraries: libpq */
//==============================================================================
// File:	m_pgsql.cpp
// Purpose: Provide interface to postgresql
//==============================================================================
#include "m_pgsql.h"

//------------------------------------------------------------------------------
// PgSQLModule
//------------------------------------------------------------------------------
PgSQLModule::PgSQLModule(const Anope::string& _name, const Anope::string& _creator)
  : Module(_name, _creator, EXTRA | VENDOR)
{
    
}

//------------------------------------------------------------------------------
PgSQLModule::~PgSQLModule()
{
  // Disconnect
  for (std::map<Anope::string, PgSQLConnection*>::iterator hCurrentConnection = m_connections.begin(); hCurrentConnection != m_connections.end(); ++hCurrentConnection)
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
  for (std::map<Anope::string, PgSQLConnection*>::iterator hCurrentConnection = m_connections.begin(); hCurrentConnection != m_connections.end(); ++hCurrentConnection)
  {
    delete hCurrentConnection->second;
    Log(LOG_NORMAL, "pgsql") << "PgSQL: Removing server connection " << hCurrentConnection->first;
  }
  m_connections.clear();
  
  // Connect
  Configuration::Block* pBlock = _pConfig->GetModule(this);
  for (int i = 0; i < pBlock->CountBlock("pgsql"); ++i)
  {
    Configuration::Block* pPgSQLBlock = pBlock->GetBlock("pgsql", i);
    const Anope::string& connectionName = pPgSQLBlock->Get<const Anope::string>("name", "pgsql/main");

    if (this->m_connections.find(connectionName) == this->m_connections.end())
    {
      const Anope::string &user     = pPgSQLBlock->Get<const Anope::string>("username", "anope");
      const Anope::string &password = pPgSQLBlock->Get<const Anope::string>("password");
      const Anope::string &server   = pPgSQLBlock->Get<const Anope::string>("server", "127.0.0.1");
      const Anope::string &port     = pPgSQLBlock->Get<const Anope::string>("port", "5432");
      const Anope::string &database = pPgSQLBlock->Get<const Anope::string>("database", "anope");
      const Anope::string &schema   = pPgSQLBlock->Get<const Anope::string>("schema", "public");
      
      try
      {
        PgSQLConnection* pConnection = new PgSQLConnection(this, connectionName, database, server, user, password, port);
        this->m_connections.insert(std::make_pair(connectionName, pConnection));

        Log(LOG_NORMAL, "pgsql") << "PgSQL: Successfully connected to server " << connectionName << " (" << server << ")";
      }
      catch (const Datastore::Exception& exception)
      {
        Log(LOG_NORMAL, "pgsql") << "PgSQL: " << exception.GetReason();
      }
    }
  }
}

//------------------------------------------------------------------------------
void PgSQLModule::OnNotify() anope_override
{
  
}

//------------------------------------------------------------------------------
// PgSQLConnection
//------------------------------------------------------------------------------
void PgSQLConnection::Connect()
{
  // TODO: Add Timeout
  // "-c connect_timeout=5"
  m_pConnection = PQsetdbLogin(m_hostname.c_str(), m_port.c_str(), NULL, NULL, m_database.c_str(), m_username.c_str(), m_password.c_str());

  if (!m_pConnection || PQstatus(m_pConnection) == CONNECTION_BAD)
    throw Datastore::Exception("Unable to connect to the postgres server " + this->name + ": " + PQerrorMessage(m_pConnection));

  Log(LOG_DEBUG) << "Successfully connected to the postgres server " << this->name << " at " << this->m_hostname << ":" << this->m_port;
}

//------------------------------------------------------------------------------
void PgSQLConnection::Disconnect()
{
  PQfinish(m_pConnection);
  m_pConnection = NULL;
}

//------------------------------------------------------------------------------
bool PgSQLConnection::isConnected()
{
  if (!m_pConnection || (PQstatus(m_pConnection) == CONNECTION_BAD))
  {
    try
    {
      Connect();
    }
    catch (const Datastore::Exception &)
    {
      Log(LOG_DEBUG) << "PgSQL: " << PQerrorMessage(m_pConnection);
      return false;
    }
  }
  
  return true;
}

//------------------------------------------------------------------------------
Anope::string PgSQLConnection::EscapeQuery(const Anope::string& _rawQuery)
{
  char* pEscapedBuffer = new char[_rawQuery.length() * 2 + 1]();
  int error;
  
  PQescapeStringConn(m_pConnection, pEscapedBuffer, _rawQuery.c_str(), _rawQuery.length(), &error);
  
  if (error)
    Log(LOG_DEBUG) << "PgSQL: " << PQerrorMessage(m_pConnection);

  Anope::string escapedQuery(pEscapedBuffer);
  delete[] pEscapedBuffer;
  
  return escapedQuery;
}

//------------------------------------------------------------------------------
Result PgSQLConnection::Query(const Anope::string& _rawQuery)
{
  if(!isConnected())
    return Result();
  
  PGresult* pResult = PQexec(m_pConnection, _rawQuery.c_str());
  
  if(pResult)
    Log(LOG_DEBUG) << "PgSQL: " << PQresultErrorMessage(pResult);

  return Result();
}

//------------------------------------------------------------------------------
Anope::string PgSQLConnection::BuildCreateTableQuery(Serializable* _pObject)
{
  Data serialized_data;
  _pObject->Serialize(serialized_data);

  Anope::string rawQuery = "";
  rawQuery += "CREATE TABLE IF NOT EXISTS \"";
  rawQuery += _pObject->GetSerializableType()->GetName();
  rawQuery += "\" (";
  rawQuery += "\"id\" serial primary key, ";
  
  for (Data::Map::const_iterator it = serialized_data.data.begin(), it_end = serialized_data.data.end(); it != it_end; ++it)
  {
    if(strcmp(it->first.c_str(), "id") == 0)
      continue;
    
    rawQuery += "\"";
    rawQuery += it->first;
    rawQuery += "\" character varying, ";
  }
  
  rawQuery += "\"created_at\" timestamp NOT NULL, \"updated_at\" timestamp NOT NULL); ";
  
  return rawQuery;
}

//------------------------------------------------------------------------------
Anope::string PgSQLConnection::BuildInsertRowQuery(Serializable* _pObject)
{
  Data serialized_data;
  _pObject->Serialize(serialized_data);
  
  Anope::string rawQuery = "";
  rawQuery += "INSERT INTO \"";
  rawQuery += _pObject->GetSerializableType()->GetName();
  rawQuery += "\" (";
  
  for (Data::Map::const_iterator it = serialized_data.data.begin(), it_end = serialized_data.data.end(); it != it_end; ++it)
  {
    if(strcmp(it->first.c_str(), "id") == 0)
      continue;
    
    rawQuery += "\"";
    rawQuery += it->first;
    rawQuery += "\", ";
  }
  
  rawQuery += "\"created_at\", \"updated_at\") VALUES (";
  
  
  for (Data::Map::const_iterator it = serialized_data.data.begin(), it_end = serialized_data.data.end(); it != it_end; ++it)
  {
    if(strcmp(it->first.c_str(), "id") == 0)
      continue;

    Anope::string buffer;
    *it->second >> buffer;
    
    rawQuery += "'";
    rawQuery += EscapeQuery(buffer);
    rawQuery += "', ";
  }
  
  rawQuery += "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING \"id\"; ";
  
  return rawQuery;
}

//------------------------------------------------------------------------------
PgSQLConnection::PgSQLConnection(Module* _pOwner, const Anope::string& _name, const Anope::string& _database, const Anope::string& _hostname, const Anope::string& _username, const Anope::string& _password, const Anope::string& _port)
  : Provider(_pOwner, _name),
  m_username(_username),
  m_password(_password),
  m_hostname(_hostname),
  m_port(_port),
  m_database(_database),
  m_pConnection(NULL)
{
  Connect();
}

//------------------------------------------------------------------------------
PgSQLConnection::~PgSQLConnection()
{
  Disconnect();
}

//------------------------------------------------------------------------------
Result PgSQLConnection::Create(Serializable* _pObject) anope_override
{
  if(_pObject->id != 0)
    return Update(_pObject);
  
  return Query(BuildCreateTableQuery(_pObject) + BuildInsertRowQuery(_pObject));

  //_pObject->UpdateTS();
  //m_updatedItems.insert(_pObject);
  //this->Notify();
}

//------------------------------------------------------------------------------
Result PgSQLConnection::Read(Serializable* _pObject) anope_override
{
  Log(LOG_DEBUG) << "DBSQL::Read";
  
  Result result;
  
  //   Query query("SELECT * FROM \"" + _pObject->GetName() + "\" WHERE (\"timestamp\" >= " + m_hDatabaseConnection->FromUnixtime(_pObject->GetTimestamp()) + " OR \"timestamp\" IS NULL)");

//   _pObject->UpdateTimestamp();

//   Result res = this->RunQuery(query);

//   bool clear_null = false;
//   for (int i = 0; i < res.Rows(); ++i)
//   {
//     const std::map<Anope::string, Anope::string> &row = res.Row(i);

//     unsigned int id;
//     try
//     {
//       id = convertTo<unsigned int>(res.Get(i, "id"));
//     }
//     catch (const ConvertException &)
//     {
//       Log(LOG_DEBUG) << "Unable to convert id from " << _pObject->GetName();
//       continue;
//     }

//     if (res.Get(i, "timestamp").empty())
//     {
//       clear_null = true;
//       std::map<uint64_t, Serializable *>::iterator it = _pObject->objects.find(id);
//       if (it != _pObject->objects.end())
//         delete it->second; // This also removes this object from the map
//     }
//     else
//     {
//       Data data;

//       for (std::map<Anope::string, Anope::string>::const_iterator it = row.begin(), it_end = row.end(); it != it_end; ++it)
//         data[it->first] << it->second;

//       Serializable *s = NULL;
//       std::map<uint64_t, Serializable *>::iterator it = _pObject->objects.find(id);
//       if (it != _pObject->objects.end())
//         s = it->second;

//       Serializable *new_s = _pObject->Unserialize(s, data);
//       if (new_s)
//       {
//         // If s == new_s then s->id == new_s->id
//         if (s != new_s)
//         {
//           new_s->id = id;
//           _pObject->objects[id] = new_s;

//           /* The Unserialize operation is destructive so rebuild the data for UpdateCache.
//            * Also the old data may contain columns that we don't use, so we reserialize the
//            * object to know for sure our cache is consistent
//            */

//           Data data2;
//           new_s->Serialize(data2);
//           new_s->UpdateCache(data2); /* We know this is the most up to date copy */
//         }
//       }
//       else
//       {
//         if (!s)
//           this->RunQuery("UPDATE \"" + _pObject->GetName() + "\" SET \"timestamp\" = " + m_hDatabaseConnection->FromUnixtime(_pObject->GetTimestamp()) + " WHERE \"id\" = " + stringify(id));
//         else
//           delete s;
//       }
//     }
//   }

//   if (clear_null)
//   {
//     query = "DELETE FROM \"" + _pObject->GetName() + "\" WHERE \"timestamp\" IS NULL";
//     this->RunQuery(query);
//   }
  
  
  return result;
}

//------------------------------------------------------------------------------
Result PgSQLConnection::Update(Serializable* _pObject) anope_override
{
  if(_pObject->id == 0)
    return Create(_pObject);
  
  Result result;
  
  Log(LOG_DEBUG) << "DBSQL::Update - " << _pObject->GetSerializableType()->GetName() << ":" << stringify(_pObject->id);

  if (_pObject->IsTSCached())
    return result;
  
  Data data;
  _pObject->Serialize(data);
  for (Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
  {
    Log(LOG_DEBUG) << "\t" << it->first << ":" << it->second;
  }
  
//   _pObject->UpdateTS();
//   m_updatedItems.insert(_pObject);
//   this->Notify();

// DBSQL::Update - BotInfo:0                                                                                                                                                       
//       created:1426987258                                                                                                                                                        
//       host:services.lastmile                                                                                                                                                    
//       nick:ChanServ                                                                                                                                                             
//       oper_only:0                                                                                                                                                               
//       realname:Channel Registration Service                                                                                                                                     
//       user:services    
  
  
//    t.datetime "created_at", null: false
//    t.datetime "updated_at", null: false
      
  return result;
}

//------------------------------------------------------------------------------
Result PgSQLConnection::Destroy(Serializable* _pObject) anope_override
{
  Result result;
  
  Log(LOG_DEBUG) << "DBSQL::Destroy - " << _pObject->GetSerializableType()->GetName() << ":" << stringify(_pObject->id);
  
  Data data;
  _pObject->Serialize(data);
  for (Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
  {
    Anope::string buf;
    *it->second >> buf;
    Log(LOG_DEBUG) << "\t" << it->first << ":" << buf;
  }

  //  Serialize::Type *s_type = _pObject->GetSerializableType();
  //  if (s_type)
  //  {
  //    if (_pObject->id > 0)
  //      this->RunQuery("DELETE FROM \"" + s_type->GetName() + "\" WHERE \"id\" = " + stringify(_pObject->id));

  //    s_type->objects.erase(_pObject->id);
  //  }
  //  m_updatedItems.erase(_pObject);
  
  
  return result;
}