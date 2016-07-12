#ifndef BASE_XLIB_XENTRY_H_
#define BASE_XLIB_XENTRY_H_
#include "xlib/xDefine.h"
#include "xlib/xTools.h"
#include "xlib/xNoncopyable.h"

struct xEntryC
{
  public:
    xEntryC()
    {
      id_ = 0;
      bzero(name_, sizeof(name_));
      tempid_ = 0;
    }
    virtual ~xEntryC() {}

  public:
    void set_id(QWORD id) { id_ = id; }
    QWORD id()const { return id_; }
    void set_name(const char *name)
    {
      if (NULL == name) return;
      bzero(name_, sizeof(name_));
      strncpy(name_, name, sizeof(name_));
    }
    const char* name()const { return name_; }
    void set_tempid(QWORD tempid) { tempid_ = tempid; }
    QWORD tempid()const { return tempid_; }

  private:
    QWORD id_;
    char name_[MAX_NAMESIZE];
    QWORD tempid_;
};

struct xEntry : public xEntryC, private xNoncopyable
{
  xEntry()
  {
  }
  virtual ~xEntry()
  {
  }
};
#endif  // BASE_XLIB_XENTRY_H_

