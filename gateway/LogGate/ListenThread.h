#pragma once
#include "xThread.h"
#include "xDefine.h"
#include "Socket.h"
#include "xTime.h"
#include <netinet/in.h>
#include <sys/epoll.h>

class LogGate;

class ListenThread : public xThread
{
  public:
    ListenThread(LogGate *s);
    ~ListenThread();

    bool start() { return -1 != listen_epfd; }
    bool thread_init();
    void thread_proc();
    void thread_stop();

    INT getListenEpfd() const {return listen_epfd;}
    INT getListenPort() const {return listen_addr.getPort();}

  public:
    NetAddr listen_addr;
    INT listen_sock;
    INT listen_epfd;
    struct epoll_event listen_ev[MAX_SERVER_EVENT];

    LogGate *pServer;
};

