#include "TaskThread.h"
#include "LogGate.h"
#include "NetProcessor.h"
#include <arpa/inet.h>

TaskThread::TaskThread(LogGate *s, NetProcessor* np)
:listen_epfd(-1),pServer(s),_np(np)
{
	bzero(name, sizeof(name));
}

TaskThread::~TaskThread()
{
	if (-1!=listen_epfd)
	{
		std::stringstream str;
		str.str("");
		str << name << ":TaskThread:listen_epfd";
		SAFE_CLOSE_SOCKET(listen_epfd, str.str().c_str());
	}
}

void TaskThread::thread_stop()
{
    XDBG("[TaskThread],%s,epfd:%d,stop", name, listen_epfd);

    xThread::thread_stop();
}

bool TaskThread::thread_init()
{
	listen_epfd = epoll_create(256);
	if (listen_epfd<0)
	{
        XERR("[监听],epoll_create() failed %s", strerror(errno));
		listen_epfd = -1;
        return false;
	}
	XDBG("[Task],创建epfd:%d", listen_epfd);

	return true;
}

void TaskThread::thread_proc()
{
	thread_setState(THREAD_RUN);

	while (thread_getState()==xThread::THREAD_RUN)
	{
		xTime frameTimer;

		pServer->select_th(listen_epfd, 0, listen_ev, _np);

		QWORD _e = frameTimer.uElapse();
		if (_e < TASK_THREAD_FRAME_TIME)
		{
			usleep(TASK_THREAD_FRAME_TIME - _e);
		}
		else if (_e >= WARN_SERVER_FRAME_TIME)
		{
			XLOG("[Task],帧耗时 %llu 微秒", _e);
		}
		//XDBG("[Task],帧耗时1 %llu 微秒", _e);
	}
    XDBG("[Task],%s,TaskThread stop,%p",name,this);
    _np->delClientEpoll();
    _np->delAllServerEpoll();
    _np->disconnect();
}

