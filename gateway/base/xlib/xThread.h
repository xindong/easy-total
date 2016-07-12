#pragma once
#include <pthread.h>
#include "xDefine.h"

class xThread
{
	public:
		enum ThreadState
		{
			THREAD_INIT = 0,//after new, before start()
			THREAD_RUN,//in while()
			THREAD_STOP,//stopping
			THREAD_FINISH,//can delete
		};

		xThread();
		virtual ~xThread() {}

		bool thread_start();
		virtual bool thread_init(){return true;}
		virtual void thread_stop();

		void thread_setState(ThreadState s) {state = s;}
		ThreadState thread_getState( ){return state;}

		static void *thread_run(void *param);
		void setName(const char *n)
		{
			if (!n) return;
			bzero(name, sizeof(name));
			strncpy(name, n, MAX_NAMESIZE-1);
		}
		char name[MAX_NAMESIZE];

	protected:

		volatile ThreadState state;
		virtual void thread_proc()=0;
	
	private:
		pthread_t pid;
};

