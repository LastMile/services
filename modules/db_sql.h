//==============================================================================
// File:	db_sql.h
// Purpose: Provide bi-directional commication based on sql store
//==============================================================================
#pragma once

#include "module.h"
#include "datastore.h"

//------------------------------------------------------------------------------
// DBSQL
//------------------------------------------------------------------------------
class DBSQL : public Module, public Pipe
{
  ServiceReference<Datastore::Provider> m_hDatabaseConnection;
  bool m_isDatabaseLoaded;
  bool isConnectionReady();
  
  enum EACTION { CREATE, UPDATE };
  std::map<Serializable*, EACTION> m_changeList;

 public:
  DBSQL(const Anope::string& _modname, const Anope::string& _creator);

  EventReturn OnLoadDatabase() anope_override;
  void OnShutdown() anope_override;
  void OnRestart() anope_override;
  void OnReload(Configuration::Conf* _pConfig) anope_override;
  void OnNotify() anope_override;

  void OnSerializableConstruct(Serializable* _pObject) anope_override;
  void OnSerializeCheck(Serialize::Type* _pType) anope_override;
  void OnSerializableUpdate(Serializable* _pObject) anope_override;
  void OnSerializableDestruct(Serializable* _pObject) anope_override;
};

//------------------------------------------------------------------------------
MODULE_INIT(DBSQL)
