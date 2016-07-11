#include <list>
#include "LogGate.h"
#include "NetProcessor.h"

//主线程
void LogGate::v_timetick(QWORD usec)
{
  DWORD cur = usec/1000/1000;

  _close_iter tmp_close;
  NetProcessor *clo = 0;
  {
    ScopeWriteLock swl(_close_critical);
    for (_close_iter it = _close_list.begin(); it != _close_list.end(); )
    {
      tmp_close = it++;

      if (tmp_close->second + 3 <= cur)
      {
        clo = tmp_close->first;
        v_CloseNp(clo);

        XDBG("[%s],_close_list 删除连接,%s(%llu),%p", serverName, clo->name(), clo->id(), clo);
        _close_list.erase(tmp_close);
        SAFE_DELETE(clo);
      }
    }
  }
}
