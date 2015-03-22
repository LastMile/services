//==============================================================================
// File:	db_sql.cpp
// Purpose: Provide bi-directional commication based on sql store
//==============================================================================
#include "db_sql.h"

//------------------------------------------------------------------------------
bool DBSQL::isConnectionReady()
{
  if (m_isDatabaseLoaded && m_hDatabaseConnection)
  {
    if (Anope::ReadOnly)
    {
      Anope::ReadOnly = false;
      Log() << "Database Ready";
    }

    return true;
  }
  else
  {
    Log() << "Failed to connect to database - Read Only Mode Active";
    Anope::ReadOnly = true;
    
    return false;
  }
}

//------------------------------------------------------------------------------
DBSQL::DBSQL(const Anope::string& _modname, const Anope::string& _creator)
  : Module(_modname, _creator, DATABASE | VENDOR),
  m_hDatabaseConnection("", ""),
  m_isDatabaseLoaded(false)
{
  if (ModuleManager::FindFirstOf(DATABASE) != this)
    throw ModuleException("If db_sql is loaded it must be the first database module loaded.");
}

//------------------------------------------------------------------------------
void DBSQL::OnNotify() anope_override
{
  if (!this->isConnectionReady())
    return;
  
  Log(LOG_DEBUG) << "DBSQL::OnNotify";

  // Data
  // next if IsCached
  // UpdateCache
  // next if no Serialize::Type
  // Save
  // if new item update ID
  
//   std::set<Serializable*>::iterator itemsIterator;
//   for (itemsIterator = m_updatedItems.begin(); itemsIterator != m_updatedItems.end(); ++itemsIterator)
//   {
//     Serializable* pObject = *itemsIterator;
//     Data data;

//     pObject->Serialize(data);

//     if (pObject->IsCached(data))
//       continue;

//     pObject->UpdateCache(data);

//     Serialize::Type *s_type = pObject->GetSerializableType();

//     if (!s_type)
//       continue;

//     // TODO: Move the concerns of prefix to the database service
//     std::vector<Query> create = m_hDatabaseConnection->CreateTable(s_type->GetName(), data);

//     for (unsigned int i = 0; i < create.size(); ++i)
//       this->RunQuery(create[i]);

//     Result res = this->RunQuery(m_hDatabaseConnection->BuildInsert(s_type->GetName(), pObject->id, data));

//     if (res.GetID() && pObject->id != res.GetID())
//     {
//       /* In this case object is new, so place it into the object map */
//       pObject->id = res.GetID();
//       s_type->objects[pObject->id] = pObject;
//     }
//   }

//   m_updatedItems.clear();
}

//------------------------------------------------------------------------------
EventReturn DBSQL::OnLoadDatabase() anope_override
{
  m_isDatabaseLoaded = true;
  return EVENT_STOP;
}

//------------------------------------------------------------------------------
void DBSQL::OnShutdown() anope_override
{
  m_isDatabaseLoaded = false;
}

//------------------------------------------------------------------------------
void DBSQL::OnRestart() anope_override
{
  m_isDatabaseLoaded = false;
}

//------------------------------------------------------------------------------
void DBSQL::OnReload(Configuration::Conf* _pConfig) anope_override
{
  Configuration::Block* pBlock = _pConfig->GetModule(this);
  m_hDatabaseConnection = ServiceReference<Datastore::Provider>("Datastore::Provider", pBlock->Get<const Anope::string>("engine"));
}

//------------------------------------------------------------------------------
void DBSQL::OnSerializableConstruct(Serializable* _pObject) anope_override
{
  if (!this->isConnectionReady())
    return;

  m_hDatabaseConnection->Create(_pObject);
  
  //_pObject->UpdateTS();
}

//------------------------------------------------------------------------------
void DBSQL::OnSerializeCheck(Serialize::Type* _pObject) anope_override
{
  if (!this->isConnectionReady())
    return;
  
  m_hDatabaseConnection->Read(NULL);
}

//------------------------------------------------------------------------------
void DBSQL::OnSerializableUpdate(Serializable* _pObject) anope_override
{
  if (!this->isConnectionReady())
    return;
  
  m_hDatabaseConnection->Update(_pObject);
  
  // _pObject->UpdateTS();
}

//------------------------------------------------------------------------------
void DBSQL::OnSerializableDestruct(Serializable* _pObject) anope_override
{
  if (!this->isConnectionReady())
    return;
  
  m_hDatabaseConnection->Destroy(_pObject);
  
  //  Serialize::Type *s_type = _pObject->GetSerializableType();
  //   s_type->objects.erase(_pObject->id);
}

