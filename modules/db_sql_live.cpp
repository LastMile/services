//==============================================================================
// File:	db_sql_live.cpp
// Purpose: Provide bi-directional commication based on sql store
//==============================================================================
#include "module.h"
#include "modules/sql.h"

using namespace SQL;

//------------------------------------------------------------------------------
// DBSQLLive
//------------------------------------------------------------------------------
class DBSQLLive : public Module, public Pipe
{
  std::set<Serializable*> m_updatedItems;
	Anope::string m_prefix;
  
	ServiceReference<Provider> m_service;
	time_t m_lastwarn;
	bool m_readyOnly;
	bool m_init;
	

	bool CheckSQL();
	bool CheckInit();

	void RunQuery(const Query &query);
	Result RunQueryResult(const Query &query);

 public:
	DBSQLLive(const Anope::string &modname, const Anope::string &creator);

	EventReturn OnLoadDatabase() anope_override;
	void OnShutdown() anope_override;
	void OnRestart() anope_override;
	void OnReload(Configuration::Conf *conf) anope_override;
  void OnNotify() anope_override;

	void OnSerializableConstruct(Serializable *obj) anope_override;
	void OnSerializableDestruct(Serializable *obj) anope_override;
	void OnSerializeCheck(Serialize::Type *obj) anope_override;
	void OnSerializableUpdate(Serializable *obj) anope_override;
};

//------------------------------------------------------------------------------
bool DBSQLLive::CheckSQL()
{
  if (m_service)
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
bool DBSQLLive::CheckInit()
{
  return m_init && m_service;
}

//------------------------------------------------------------------------------
void DBSQLLive::RunQuery(const Query &query)
{
  /* Can this be threaded? */
  this->RunQueryResult(query);
}

//------------------------------------------------------------------------------
Result DBSQLLive::RunQueryResult(const Query &query)
{
  if (this->CheckSQL())
  {
    Result res = m_service->RunQuery(query);
    if (!res.GetError().empty())
      Log(LOG_DEBUG) << "SQL-live got error " << res.GetError() << " for " + res.finished_query;
    else
      Log(LOG_DEBUG) << "SQL-live got " << res.Rows() << " rows for " << res.finished_query;
    return res;
  }
  throw SQL::Exception("No SQL!");
}

//------------------------------------------------------------------------------
DBSQLLive::DBSQLLive(const Anope::string& modname, const Anope::string &creator) 
  : Module(modname, creator, DATABASE | VENDOR), m_service("", "")
{
  m_lastwarn = 0;
  m_readyOnly = false;
  m_init = false;


  if (ModuleManager::FindFirstOf(DATABASE) != this)
    throw ModuleException("If db_sql_live is loaded it must be the first database module loaded.");
}

//------------------------------------------------------------------------------
void DBSQLLive::OnNotify() anope_override
{
  if (!this->CheckInit())
    return;

  for (std::set<Serializable *>::iterator it = m_updatedItems.begin(), it_end = m_updatedItems.end(); it != it_end; ++it)
  {
    Serializable *obj = *it;

    if (obj && m_service)
    {
      Data data;
      obj->Serialize(data);

      if (obj->IsCached(data))
        continue;

      obj->UpdateCache(data);

      Serialize::Type *s_type = obj->GetSerializableType();
      if (!s_type)
        continue;

      std::vector<Query> create = m_service->CreateTable(m_prefix + s_type->GetName(), data);
      for (unsigned i = 0; i < create.size(); ++i)
        this->RunQueryResult(create[i]);

      Result res = this->RunQueryResult(m_service->BuildInsert(m_prefix + s_type->GetName(), obj->id, data));
      if (res.GetID() && obj->id != res.GetID())
      {
        /* In this case obj is new, so place it into the object map */
        obj->id = res.GetID();
        s_type->objects[obj->id] = obj;
      }
    }
  }

  m_updatedItems.clear();
}

//------------------------------------------------------------------------------
EventReturn DBSQLLive::OnLoadDatabase() anope_override
{
  m_init = true;
  return EVENT_STOP;
}

//------------------------------------------------------------------------------
void DBSQLLive::OnShutdown() anope_override
{
  m_init = false;
}

//------------------------------------------------------------------------------
void DBSQLLive::OnRestart() anope_override
{
  m_init = false;
}

//------------------------------------------------------------------------------
void DBSQLLive::OnReload(Configuration::Conf *conf) anope_override
{
  Configuration::Block *block = conf->GetModule(this);
  m_service = ServiceReference<Provider>("SQL::Provider", block->Get<const Anope::string>("engine"));
  m_prefix = block->Get<const Anope::string>("prefix", "anope_db_");
}

//------------------------------------------------------------------------------
void DBSQLLive::OnSerializableConstruct(Serializable *obj) anope_override
{
  if (!this->CheckInit())
    return;
  obj->UpdateTS();
  m_updatedItems.insert(obj);
  this->Notify();
}

//------------------------------------------------------------------------------
void DBSQLLive::OnSerializableDestruct(Serializable *obj) anope_override
{
  if (!this->CheckInit())	
    return;
  Serialize::Type *s_type = obj->GetSerializableType();
  if (s_type)
  {
    if (obj->id > 0)
      this->RunQuery("DELETE FROM \"" + m_prefix + s_type->GetName() + "\" WHERE \"id\" = " + stringify(obj->id));
    s_type->objects.erase(obj->id);
  }
  m_updatedItems.erase(obj);
}

//------------------------------------------------------------------------------
void DBSQLLive::OnSerializeCheck(Serialize::Type *obj) anope_override
{
  if (!this->CheckInit() || obj->GetTimestamp() == Anope::CurTime)
    return;

  Query query("SELECT * FROM \"" + m_prefix + obj->GetName() + "\" WHERE (\"timestamp\" >= " + m_service->FromUnixtime(obj->GetTimestamp()) + " OR \"timestamp\" IS NULL)");

  obj->UpdateTimestamp();

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
      Log(LOG_DEBUG) << "Unable to convert id from " << obj->GetName();
      continue;
    }

    if (res.Get(i, "timestamp").empty())
    {
      clear_null = true;
      std::map<uint64_t, Serializable *>::iterator it = obj->objects.find(id);
      if (it != obj->objects.end())
        delete it->second; // This also removes this object from the map
    }
    else
    {
      Data data;

      for (std::map<Anope::string, Anope::string>::const_iterator it = row.begin(), it_end = row.end(); it != it_end; ++it)
        data[it->first] << it->second;

      Serializable *s = NULL;
      std::map<uint64_t, Serializable *>::iterator it = obj->objects.find(id);
      if (it != obj->objects.end())
        s = it->second;

      Serializable *new_s = obj->Unserialize(s, data);
      if (new_s)
      {
        // If s == new_s then s->id == new_s->id
        if (s != new_s)
        {
          new_s->id = id;
          obj->objects[id] = new_s;

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
          this->RunQuery("UPDATE \"" + m_prefix + obj->GetName() + "\" SET \"timestamp\" = " + m_service->FromUnixtime(obj->GetTimestamp()) + " WHERE \"id\" = " + stringify(id));
        else
          delete s;
      }
    }
  }

  if (clear_null)
  {
    query = "DELETE FROM \"" + m_prefix + obj->GetName() + "\" WHERE \"timestamp\" IS NULL";
    this->RunQuery(query);
  }
}

//------------------------------------------------------------------------------
void DBSQLLive::OnSerializableUpdate(Serializable *obj) anope_override
{
  if (!this->CheckInit() || obj->IsTSCached())
    return;
  obj->UpdateTS();
  m_updatedItems.insert(obj);
  this->Notify();
}

//------------------------------------------------------------------------------
MODULE_INIT(DBSQLLive)
