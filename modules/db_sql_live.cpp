//==============================================================================
// File:	db_sql_live.cpp
// Purpose: Provide bi-directional commication based on sql store
//==============================================================================
#include "db_sql_live.h"

//------------------------------------------------------------------------------
bool DBSQLLive::CheckSQL()
{
  if (m_hDatabaseService)
  {
    if (Anope::ReadOnly && m_readyOnly)
    {
      Anope::ReadOnly = m_readyOnly = false;
      Log() << "Found SQL again, going out of readonly mode...";
    }

    return true;
  }
  else
  {
    if (Anope::CurTime - Config->GetBlock("options")->Get<time_t>("updatetimeout", "5m") > m_lastwarn)
    {
      Log() << "Unable to locate SQL reference, going to readonly...";
      Anope::ReadOnly = m_readyOnly = true;
      m_lastwarn = Anope::CurTime;
    }

    return false;
  }
}

//------------------------------------------------------------------------------
bool DBSQLLive::isDatabaseReady()
{
  return m_isDatabaseLoaded && m_hDatabaseService;
}

//------------------------------------------------------------------------------
void DBSQLLive::RunQuery(const Query& _query)
{
  // TODO: Can this be threaded?
  this->RunQueryResult(_query);
}

//------------------------------------------------------------------------------
Result DBSQLLive::RunQueryResult(const Query& _query)
{
  if (!this->CheckSQL())
    throw SQL::Exception("No SQL!");

  Result result = m_hDatabaseService->RunQuery(_query);

  if (!result.GetError().empty())
    Log(LOG_DEBUG) << "SQL-live got error " << result.GetError() << " for " + result.finished_query;
  else
    Log(LOG_DEBUG) << "SQL-live got " << result.Rows() << " rows for " << result.finished_query;

  return result;
}

//------------------------------------------------------------------------------
DBSQLLive::DBSQLLive(const Anope::string& _modname, const Anope::string& _creator)
  : Module(_modname, _creator, DATABASE | VENDOR),
  m_hDatabaseService("", ""),
  m_isDatabaseLoaded(false),
  m_lastwarn(NULL),
  m_readyOnly(false)
{
  if (ModuleManager::FindFirstOf(DATABASE) != this)
    throw ModuleException("If db_sql_live is loaded it must be the first database module loaded.");
}

//------------------------------------------------------------------------------
void DBSQLLive::OnNotify() anope_override
{
  if (!this->isDatabaseReady())
    return;

  std::set<Serializable*>::iterator itemsIterator;
  for (itemsIterator = m_updatedItems.begin(); itemsIterator != m_updatedItems.end(); ++itemsIterator)
  {
    Serializable* pObject = *itemsIterator;
    Data data;

    pObject->Serialize(data);

    if (pObject->IsCached(data))
      continue;

    pObject->UpdateCache(data);

    Serialize::Type *s_type = pObject->GetSerializableType();

    if (!s_type)
      continue;

    // TODO: Move the concerns of prefix to the database service
    std::vector<Query> create = m_hDatabaseService->CreateTable(s_type->GetName(), data);

    for (unsigned int i = 0; i < create.size(); ++i)
      this->RunQueryResult(create[i]);

    Result res = this->RunQueryResult(m_hDatabaseService->BuildInsert(s_type->GetName(), pObject->id, data));

    if (res.GetID() && pObject->id != res.GetID())
    {
      /* In this case object is new, so place it into the object map */
      pObject->id = res.GetID();
      s_type->objects[pObject->id] = pObject;
    }
  }

  m_updatedItems.clear();
}

//------------------------------------------------------------------------------
EventReturn DBSQLLive::OnLoadDatabase() anope_override
{
  m_isDatabaseLoaded = true;
  return EVENT_STOP;
}

//------------------------------------------------------------------------------
void DBSQLLive::OnShutdown() anope_override
{
  m_isDatabaseLoaded = false;
}

//------------------------------------------------------------------------------
void DBSQLLive::OnRestart() anope_override
{
  m_isDatabaseLoaded = false;
}

//------------------------------------------------------------------------------
void DBSQLLive::OnReload(Configuration::Conf* _pConfig) anope_override
{
  Configuration::Block* pBlock = _pConfig->GetModule(this);
  m_hDatabaseService = ServiceReference<Provider>("SQL::Provider", pBlock->Get<const Anope::string>("engine"));
}

//------------------------------------------------------------------------------
void DBSQLLive::OnSerializableConstruct(Serializable* _pObject) anope_override
{
  if (!this->isDatabaseReady())
    return;

  _pObject->UpdateTS();
  m_updatedItems.insert(_pObject);
  this->Notify();
}

//------------------------------------------------------------------------------
void DBSQLLive::OnSerializableDestruct(Serializable* _pObject) anope_override
{
  if (!this->isDatabaseReady())
    return;

  Serialize::Type *s_type = _pObject->GetSerializableType();
  if (s_type)
  {
    if (_pObject->id > 0)
      this->RunQuery("DELETE FROM \"" + s_type->GetName() + "\" WHERE \"id\" = " + stringify(_pObject->id));

    s_type->objects.erase(_pObject->id);
  }
  m_updatedItems.erase(_pObject);
}

//------------------------------------------------------------------------------
void DBSQLLive::OnSerializeCheck(Serialize::Type* _pObject) anope_override
{
  if (!this->isDatabaseReady() || _pObject->GetTimestamp() == Anope::CurTime)
    return;

  Query query("SELECT * FROM \"" + _pObject->GetName() + "\" WHERE (\"timestamp\" >= " + m_hDatabaseService->FromUnixtime(_pObject->GetTimestamp()) + " OR \"timestamp\" IS NULL)");

  _pObject->UpdateTimestamp();

  Result res = this->RunQueryResult(query);

  bool clear_null = false;
  for (int i = 0; i < res.Rows(); ++i)
  {
    const std::map<Anope::string, Anope::string> &row = res.Row(i);

    unsigned int id;
    try
    {
      id = convertTo<unsigned int>(res.Get(i, "id"));
    }
    catch (const ConvertException &)
    {
      Log(LOG_DEBUG) << "Unable to convert id from " << _pObject->GetName();
      continue;
    }

    if (res.Get(i, "timestamp").empty())
    {
      clear_null = true;
      std::map<uint64_t, Serializable *>::iterator it = _pObject->objects.find(id);
      if (it != _pObject->objects.end())
        delete it->second; // This also removes this object from the map
    }
    else
    {
      Data data;

      for (std::map<Anope::string, Anope::string>::const_iterator it = row.begin(), it_end = row.end(); it != it_end; ++it)
        data[it->first] << it->second;

      Serializable *s = NULL;
      std::map<uint64_t, Serializable *>::iterator it = _pObject->objects.find(id);
      if (it != _pObject->objects.end())
        s = it->second;

      Serializable *new_s = _pObject->Unserialize(s, data);
      if (new_s)
      {
        // If s == new_s then s->id == new_s->id
        if (s != new_s)
        {
          new_s->id = id;
          _pObject->objects[id] = new_s;

          /* The Unserialize operation is destructive so rebuild the data for UpdateCache.
           * Also the old data may contain columns that we don't use, so we reserialize the
           * object to know for sure our cache is consistent
           */

          Data data2;
          new_s->Serialize(data2);
          new_s->UpdateCache(data2); /* We know this is the most up to date copy */
        }
      }
      else
      {
        if (!s)
          this->RunQuery("UPDATE \"" + _pObject->GetName() + "\" SET \"timestamp\" = " + m_hDatabaseService->FromUnixtime(_pObject->GetTimestamp()) + " WHERE \"id\" = " + stringify(id));
        else
          delete s;
      }
    }
  }

  if (clear_null)
  {
    query = "DELETE FROM \"" + _pObject->GetName() + "\" WHERE \"timestamp\" IS NULL";
    this->RunQuery(query);
  }
}

//------------------------------------------------------------------------------
void DBSQLLive::OnSerializableUpdate(Serializable* _pObject) anope_override
{
  if (!this->isDatabaseReady() || _pObject->IsTSCached())
    return;

  _pObject->UpdateTS();
  m_updatedItems.insert(_pObject);
  this->Notify();
}
