#pragma once
#include <stdlib.h>
#include "log.h"


#ifdef _DEBUG
	#define _MYNEW_
	#define NEW new(__FILE__, __LINE__)
	#define DELETE(p) XDELETE(p)
	#define DELETE_VEC(p) XDELETE_VEC(p)
#else
	#define NEW new
	#define DELETE(p) SAFE_DELETE(p)
	#define DELETE_VEC(p) SAFE_DELETE_VEC(p)
#endif

struct Meminfo
{
	std::string file;
	int line;
	Meminfo()
	{
		line = 0;
	}
};
struct MemSta
{
	public:
		static void add_use_mem(const void* p, const char* file, int line)
		{
			Meminfo info;
			info.file = file;
			info.line = line;
			Mems[p]=info;
			//XLOG("[memory],add %p,file:%s,line:%d,total:%u",p,file,line,Mems.size());
		}
		static void del_use_mem(const void* p)
		{
			if(Mems.find(p)==Mems.end()) return;
			Mems.erase(p);
			//XLOG("[memory],erase %p,total:%u",p,Mems.size());
		}
		static void printLeakMem()
		{
			MEM::iterator it=Mems.begin(),end=Mems.end();
			for(;it!=end;it++)
			{
				XLOG("[memory],leak:%p,file:%s,line:%d",it->first,it->second.file.c_str(),it->second.line);
			}
		}
	private:
		typedef std::map<const void*, Meminfo> MEM;
		static MEM Mems;
};

#ifdef _MYNEW_
#define SAFE_DELETE(p) do { MemSta::del_use_mem(p); delete p; p = NULL;} while(false)
#define SAFE_DELETE_VEC(p) do { MemSta::del_use_mem(p); delete[] p; p = NULL;} while(false)
#else
#define SAFE_DELETE(p) do {delete p; p = NULL;} while(false)
#define SAFE_DELETE_VEC(p) do {delete[] p; p = NULL;} while(false)
#endif

inline void * operator new(size_t size, const char *file, int line)// throw (std::bad_alloc)
{
	void *p = operator new(size);
#ifdef _MYNEW_
	MemSta::add_use_mem(p,file,line);
#endif
	return p;
}

inline void* operator new[](size_t size, const char *file, int line)
{
	void * p = operator new[](size);
#ifdef _MYNEW_
	MemSta::add_use_mem(p,file,line);
#endif
	return p;
}

#define XDELETE(p) {\
		SAFE_DELETE(p);\
}

#define XDELETE_VEC(p){\
		SAFE_DELETE_VEC(p);\
}\

inline void operator delete(void* p, const char *file, int line)//throw ()
{
	//XTRC("[memory],new failed:%p,file:%s,line:%d",p,file,line);
	operator delete(p);
}

inline void operator delete [](void* p, const char *file, int line)//throw ()
{
	//XTRC("[memory],new[] failed%p,file:%s,line:%d",p,file,line);
	operator delete[](p);
}

