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

//------------------------------------------------------------------------------
// PgSQLConnection
//------------------------------------------------------------------------------
class PgSQLConnection : public Datastore::Provider
{
  Anope::string m_username;
  Anope::string m_password;
  Anope::string m_hostname;
  Anope::string m_port;
  Anope::string m_database;
  Anope::string m_schema;

  PGconn* m_pConnection;

 public:
  PgSQLConnection(Module* _pOwner, const Anope::string& _name, const Anope::string& _database, const Anope::string& _hostname, const Anope::string& _username, const Anope::string& _password, const Anope::string& _port);
  ~PgSQLConnection();

  Result Create(Serializable* _pObject) anope_override;
  Result Read(Serializable* _pObject) anope_override;
  Result Update(Serializable* _pObject) anope_override;
  Result Destroy(Serializable* _pObject) anope_override;
};

//------------------------------------------------------------------------------
MODULE_INIT(PgSQLModule)
