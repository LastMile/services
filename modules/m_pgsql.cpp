/* RequiredLibraries: pq */
/* RequiredWindowsLibraries: libpq */
//==============================================================================
// File:	m_pgsql.cpp
// Purpose: Provide interface to postgresql
//==============================================================================
#include "m_pgsql.h"

//------------------------------------------------------------------------------
// PgSQLConnection
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
  
}

//------------------------------------------------------------------------------
PgSQLConnection::~PgSQLConnection()
{
  
}

//------------------------------------------------------------------------------
Result PgSQLConnection::Create(Serializable* _pObject) anope_override
{
  Result result;
  
  Log(LOG_DEBUG) << "DBSQL::Create - " << _pObject->GetSerializableType()->GetName() << ":" << stringify(_pObject->id);
  
  Data data;
  _pObject->Serialize(data);
  for (Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
  {
    Anope::string buf;
    *it->second >> buf;
    Log(LOG_DEBUG) << "\t" << it->first << ":" << buf;
  }
  
  //_pObject->UpdateTS();
  //m_updatedItems.insert(_pObject);
  //this->Notify();
  
  
  return result;
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
  Result result;
  
  Log(LOG_DEBUG) << "DBSQL::Update - " << _pObject->GetSerializableType()->GetName() << ":" << stringify(_pObject->id);

  if (_pObject->IsTSCached())
    return result;
  
  Data data;
  _pObject->Serialize(data);
  for (Data::Map::const_iterator it = data.data.begin(), it_end = data.data.end(); it != it_end; ++it)
  {
    Anope::string buf;
    *it->second >> buf;
    Log(LOG_DEBUG) << "\t" << it->first << ":" << buf;
  }

//   _pObject->UpdateTS();
//   m_updatedItems.insert(_pObject);
//   this->Notify();

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