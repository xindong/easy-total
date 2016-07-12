#pragma once
#include "xThread.h"
#include "xDefine.h"
#include "Socket.h"
#include "xTime.h"
#include <netinet/in.h>
#include <sys/epoll.h>
#include "NetProcessor.h"

class LogGate;

class TaskThread : public xThread
{
	public:
		TaskThread(LogGate *s, NetProcessor* np);
		~TaskThread();

		bool thread_init();
		void thread_proc();
		void thread_stop();

	 	int listen_epfd;
		struct epoll_event listen_ev[MAX_SERVER_EVENT];

		LogGate *pServer;
        NetProcessor* _np;
};

