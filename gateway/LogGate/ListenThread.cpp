#include "ListenThread.h"
#include "LogGate.h"
#include "NetProcessor.h"
#include <arpa/inet.h>
#include <netinet/tcp.h>

  ListenThread::ListenThread(LogGate *s)
:listen_sock(-1),listen_epfd(-1),pServer(s)
{
}

ListenThread::~ListenThread()
{
  if (-1!=listen_sock)
    SAFE_CLOSE_SOCKET(listen_sock, "listen_sock");
  if (-1!=listen_epfd)
    SAFE_CLOSE_SOCKET(listen_epfd, "listen_epfd");
}

void ListenThread::thread_stop()
{
    xThread::thread_stop();
}

bool ListenThread::thread_init()
{
  if ((listen_sock = socket(listen_addr.getFamily(), SOCK_STREAM, 0)) <0)
  {
    XERR("[监听]socket() failed %s", strerror(errno));
    listen_sock = -1;
    return false;
  }
  XLOG("[监听],%s:%d,创建socket成功,fd:%d", listen_addr.getIP().c_str(), listen_addr.getPort(), listen_sock);

  int re = 1;
  int ret = -1;
  int nRecvBuf = MAX_BUFSIZE*2; 
  int nSendBuf = MAX_BUFSIZE*2; 
  ret = setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &re, sizeof(re));
  if (ret == -1)
  {
      XERR("[监听],%d, setsockopt SO_REUSEADDR failed, %d,%s", listen_sock, errno, strerror(errno));
      return false;
  }
  ret = setsockopt(listen_sock, SOL_SOCKET,SO_RCVBUF,(const char*)&nRecvBuf,sizeof(int));
  if (ret == -1)
  {
      XERR("[监听],%d, setsockopt SO_RCVBUF failed, %d,%s", listen_sock, errno, strerror(errno));
      return false;
  }

  ret = setsockopt(listen_sock, SOL_SOCKET,SO_SNDBUF,(const char*)&nSendBuf,sizeof(int));
  if (ret == -1)
  {
      XERR("[监听],%d, setsockopt SO_SNDBUF failed, %d,%s", listen_sock, errno, strerror(errno));
      return false;
  }

  ret = setsockopt(listen_sock, IPPROTO_TCP, TCP_NODELAY, (char *)&re, sizeof(re) );
  if (ret == -1)
  {
      XERR("[监听],%d, setsockopt TCP_NODELAY failed, %d,%s", listen_sock, errno, strerror(errno));
      return false;
  }

  listen_epfd = epoll_create(256);
  if (listen_epfd<0)
  {
    XERR("[监听],epoll_create() failed %s", strerror(errno));
    listen_epfd = -1;
    return false;
  }
  XLOG("[监听],%s:%d,fd:%d,创建epfd:%d", listen_addr.getIP().c_str(), listen_addr.getPort(), listen_sock, listen_epfd);

  if (::bind(listen_sock, (sockaddr*)(listen_addr.getSockAddr()), listen_addr.getSockAddrLen()) == -1)
  {
    XERR("[监听],%s:%d, bind() failed %s", listen_addr.getIP().c_str(), listen_addr.getPort(), strerror(errno));
    return false;
  }
  XLOG("[监听],bind %s:%d", listen_addr.getIP().c_str(), listen_addr.getPort());

  if(::listen(listen_sock, 256) == -1)
  {
    XERR("[监听],%s:%d, listen失败 %s", listen_addr.getIP().c_str(), listen_addr.getPort(), strerror(errno));
    return false;
  }

  struct epoll_event _event;
  _event.data.fd = listen_sock;
  _event.events = EPOLLIN|EPOLLERR;
  epoll_ctl(listen_epfd, EPOLL_CTL_ADD, listen_sock, &_event);

  return true;
}

void ListenThread::thread_proc()
{

  thread_setState(THREAD_RUN);

  while (thread_getState()==xThread::THREAD_RUN)
  {
    xTime frameTimer;

    pServer->select_th(listen_epfd, listen_sock, listen_ev, NULL);

    QWORD _e = frameTimer.uElapse();
    if (_e < LISTEN_THREAD_FRAME_TIME)
    {
      usleep(LISTEN_THREAD_FRAME_TIME - _e);
    }
    else
    {
    }
  }
}
