#pragma once
#include "xDefine.h"
#include "xByteBuffer.h"
#include "xlib/xMutex.h"
#include "xLog.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "xNetDefine.h"

class NetProcessor;

inline void SAFE_CLOSE_SOCKET(int &fd, const char *name)
{
	XDBG("[Socket],%s,close %d", name, fd);

	int ret = 0;

	ret = shutdown(fd, SHUT_RDWR);

	ret = close(fd);
	if (0!=ret)
		XERR("[Socket]closesocket failed fd%d ret%d", fd, ret);

	fd = -1;
}

class Socket
{
	public:
		Socket(NetProcessor *n);
		~Socket();

		int get_fd() const {return _fd;}

		bool valid(){return _fd!=-1;}
		void shutdown(int how);
		bool connect(const NetAddr* addr);
		bool accept(int sockfd, const sockaddr* addr, DWORD addr_len);
		void close();
        const NetAddr& getAddr()const {return _addr;}

		bool setNonBlock();
		bool setSockOpt();

		bool getCmd(BYTE *&cmd, DWORD &len);
		bool popCmd();

		//数据放进缓冲区，等待发送
		bool sendCmd(const void *data, DWORD len);
        bool sendJson(const void *data, WORD len);
		//返回发送后缓冲区剩余字节数
		int sendCmd();
        int sendJson();

		bool sendFlashPolicy();

		bool readToBuf();
		bool writeToBuf(void *data, DWORD len);

		inline void addEpoll(int ep)
		{
			_epfd = ep;
			epoll_event ev;
			bzero(&ev, sizeof(ev));
			ev.data.fd = _fd;
			//ev.data.ptr = _np;
			ev.events = EPOLLIN|EPOLLOUT|EPOLLET;
			epoll_ctl(ep, EPOLL_CTL_ADD, _fd, &ev);
		}

		inline void addEpoll()
		{
			struct epoll_event ev;
			bzero(&ev, sizeof(ev));
			ev.data.fd = _fd;
			ev.events = EPOLLIN|EPOLLOUT|EPOLLET;
			//ev.data.ptr = _np;
			epoll_ctl(_epfd, EPOLL_CTL_MOD, _fd, &ev);
		}

		inline void delEpoll()
		{
      XDBG("[Socket],%u,del epoll epfd:%u", _fd, _epfd);
			epoll_event ev;
			bzero(&ev, sizeof(ev));
			ev.data.fd = _fd;
			//ev.data.ptr = _np;
			ev.events = EPOLLIN|EPOLLOUT|EPOLLET;
			epoll_ctl(_epfd, EPOLL_CTL_DEL, _fd, &ev);
			_epfd = 0;
		}

	protected:

        void swapBuffer();
		void compressAll();

		int _fd;
		INT _epfd;
        NetAddr _addr;

		//发送缓存
	private:
		xByteBuffer _write_buffer;
		xRWLock _write_critical;

		xByteBuffer _read_buffer;
		xByteBuffer _cmd_write_buffer;
		xByteBuffer _tmp_cmd_write_buffer;
		xRWLock _cmd_write_critical;

		DWORD unCompCmdRealSize;//未解压的消息长度
		xByteBuffer tmpWriteBuffer;	//临时存储压缩、加密数据
		xByteBuffer tmpDecBuffer;	//存放解密后的数据
		xByteBuffer cmdBuffer;		//存放解压后的数据

		xRWLock _send_critical;

		NetProcessor *_np;
};
