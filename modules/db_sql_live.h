//==============================================================================
// File:	db_sql_live.h
// Purpose: Provide bi-directional commication based on sql store
//==============================================================================
#pragma once

#include "module.h"
#include "modules/sql.h"

using namespace SQL;

//------------------------------------------------------------------------------
// DBSQLLive
//------------------------------------------------------------------------------
class DBSQLLive : public Module, public Pipe
{
  std::set<Serializable*> m_updatedItems;

  ServiceReference<Provider> m_hDatabaseService;
  bool m_isDatabaseLoaded;

  time_t m_lastwarn;

  bool CheckSQL();
  bool isDatabaseReady();

  void RunQuery(const Query& _query);
  Result RunQueryResult(const Query& _query);

 public:
  DBSQLLive(const Anope::string& _modname, const Anope::string& _creator);

  EventReturn OnLoadDatabase() anope_override;
  void OnShutdown() anope_override;
  void OnRestart() anope_override;
  void OnReload(Configuration::Conf* _pConfig) anope_override;
  void OnNotify() anope_override;

  void OnSerializableConstruct(Serializable* _pObject) anope_override;
  void OnSerializableDestruct(Serializable* _pObject) anope_override;
  void OnSerializeCheck(Serialize::Type* _pObject) anope_override;
  void OnSerializableUpdate(Serializable* _pObject) anope_override;
};

//------------------------------------------------------------------------------
MODULE_INIT(DBSQLLive)
