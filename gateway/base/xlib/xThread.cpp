#include <unistd.h>
#include "xThread.h"
#include "xTools.h"


xThread::xThread()
{
	thread_setState(THREAD_INIT);
	pid = 0;
}

bool xThread::thread_start()
{
    if (!thread_init()) return false;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_DETACHED);
	int ret = pthread_create(&pid, &attr, &thread_run, (void *)this);
    pthread_attr_destroy(&attr);
	if (ret == 0)
	{
		//setState(THREAD_RUN);
		//pthread_join(pid,NULL);
		XDBG("创建新线程成功, id:%llu", pid);
		return true;
	}
	else
	{
		XERR("创建新线程失败 err=%d", ret);
		return false;
	}
}

void xThread::thread_stop()
{
  if (thread_getState()==THREAD_INIT)
			thread_setState(THREAD_FINISH);

	while (thread_getState()!=THREAD_FINISH)
	{
		if (thread_getState()==THREAD_RUN)
			thread_setState(THREAD_STOP);
        usleep(10000);
	}
}

void *xThread::thread_run(void *param)
{
	xThread *t = (xThread *)param;
	t->thread_proc();
	t->thread_setState(THREAD_FINISH);
	return 0;
}

