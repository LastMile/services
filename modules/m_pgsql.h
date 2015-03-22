//==============================================================================
// File:	m_pgsql.h
// Purpose: Provide interface to postgresql
//==============================================================================
#pragma once

#include "module.h"
#include "datastore.h"

#include <cstdlib>
#include <sstream>
#include <libpq-fe.h>

using namespace Datastore;
class PgSQLConnection;

//------------------------------------------------------------------------------
// PgSQLModule
//------------------------------------------------------------------------------
class PgSQLModule : public Module, public Pipe
{
  std::map<Anope::string, PgSQLConnection*> m_connections;
  
  public:
  
  PgSQLModule(const Anope::string& _name, const Anope::string& _creator);
  ~PgSQLModule();

  void OnReload(Configuration::Conf* _pConfig) anope_override;
  void OnNotify() anope_override;
};

//------------------------------------------------------------------------------
// PgSQLConnection
//------------------------------------------------------------------------------
class PgSQLConnection : public Provider
{
  Anope::string m_username;
  Anope::string m_password;
  Anope::string m_hostname;
  Anope::string m_port;
  Anope::string m_database;
  Anope::string m_schema;

  PGconn* m_pConnection;
  
  void Connect();
  void Disconnect();
  bool isConnected();
  
  void Query(const Anope::string& _rawQuery);
  
  Anope::string EscapeString(const Anope::string& _rawQuery);
  Anope::string BuildCreateTableQuery(Serializable* _pObject);
  Anope::string BuildInsertRowQuery(Serializable* _pObject);
  Anope::string BuildUpdateRowQuery(Serializable* _pObject);
  Anope::string BuildDestroyRowQuery(Serializable* _pObject);
  
 public:
  PgSQLConnection(Module* _pOwner, const Anope::string& _name, const Anope::string& _database, const Anope::string& _hostname, const Anope::string& _username, const Anope::string& _password, const Anope::string& _port);
  ~PgSQLConnection();

  void Create(Serializable* _pObject) anope_override;
  void Read(Serializable* _pObject) anope_override;
  void Update(Serializable* _pObject) anope_override;
  void Destroy(Serializable* _pObject) anope_override;
};

//------------------------------------------------------------------------------
MODULE_INIT(PgSQLModule)
