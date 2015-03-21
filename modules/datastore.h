//==============================================================================
// File:	datastore.h
// Purpose: Provide common interface for datastore and backing services
//==============================================================================
namespace Datastore
{
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
