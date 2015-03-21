//==============================================================================
// File:	datastore.h
// Purpose: Provide common interface for datastore and backing services
//==============================================================================
namespace Datastore
{
  //------------------------------------------------------------------------------
  // Exception
  //------------------------------------------------------------------------------
  class Data : public Serialize::Data
	{
	 public:
 		typedef std::map<Anope::string, std::stringstream *> Map;
		Map data;
		std::map<Anope::string, Type> types;

		~Data()
		{
			Clear();
		}

		std::iostream& operator[](const Anope::string &key) anope_override
		{
			std::stringstream *&ss = data[key];
			if (!ss)
				ss = new std::stringstream();
			return *ss;
		}

		std::set<Anope::string> KeySet() const anope_override
		{
			std::set<Anope::string> keys;
			for (Map::const_iterator it = this->data.begin(), it_end = this->data.end(); it != it_end; ++it)
				keys.insert(it->first);
			return keys;
		}

		size_t Hash() const anope_override
		{
			size_t hash = 0;
			for (Map::const_iterator it = this->data.begin(), it_end = this->data.end(); it != it_end; ++it)
				if (!it->second->str().empty())
					hash ^= Anope::hash_cs()(it->second->str());
			return hash;
		}

		std::map<Anope::string, std::iostream *> GetData() const
		{
			std::map<Anope::string, std::iostream *> d;
			for (Map::const_iterator it = this->data.begin(), it_end = this->data.end(); it != it_end; ++it)
				d[it->first] = it->second;
			return d;
		}

		void Clear()
		{
			for (Map::const_iterator it = this->data.begin(), it_end = this->data.end(); it != it_end; ++it)
				delete it->second;
			this->data.clear();
		}

		void SetType(const Anope::string &key, Type t) anope_override
		{
			this->types[key] = t;
		}

		Type GetType(const Anope::string &key) const anope_override
		{
			std::map<Anope::string, Type>::const_iterator it = this->types.find(key);
			if (it != this->types.end())
				return it->second;
			return DT_TEXT;
		}
	};
  
  //------------------------------------------------------------------------------
  // Exception
  //------------------------------------------------------------------------------
	class Exception : public ModuleException
	{
	 public:
		Exception(const Anope::string& _message) : ModuleException(_message) { }

		virtual ~Exception() throw() { }
	};

  //------------------------------------------------------------------------------
  // Result
  //------------------------------------------------------------------------------
	class Result
	{
	};

  //------------------------------------------------------------------------------
  // Provider
  //------------------------------------------------------------------------------
	class Provider : public Service
	{
	 public:
		Provider(Module* _pOwner, const Anope::string& _name) : Service(_pOwner, "Datastore::Provider", _name) { }

    virtual Result Create(Serializable* _pObject) = 0;
    virtual Result Read(Serializable* _pObject) = 0;
    virtual Result Update(Serializable* _pObject) = 0;
    virtual Result Destroy(Serializable* _pObject) = 0;
	};

}
