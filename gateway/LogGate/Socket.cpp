#include "Socket.h"
#include "NetProcessor.h"
#include <errno.h>
#include <fcntl.h>
#include "LogGate.h"
#include <netinet/tcp.h>

Socket::Socket(NetProcessor *n)
:_np(n)
{
	_fd = -1;

	_write_buffer.Resize(MAX_BUFSIZE*2);
	_read_buffer.Resize(MAX_BUFSIZE*2);
	tmpWriteBuffer.Resize(MAX_BUFSIZE);
	tmpDecBuffer.Resize(MAX_BUFSIZE);
	cmdBuffer.Resize(MAX_BUFSIZE);
	_cmd_write_buffer.Resize(MAX_BUFSIZE*2);
	_tmp_cmd_write_buffer.Resize(MAX_BUFSIZE*2);

	unCompCmdRealSize = 0;
}

Socket::~Socket()
{
	close();
}

bool Socket::accept(int sockfd, const sockaddr* addr, DWORD addr_len)
{
	_fd = sockfd;
    _addr.setIPAndPort(addr, addr_len);

	XLOG("[Socket],open,%s,%d,%s,%u", _np->name(), _fd, _addr.getIP().c_str(), _addr.getPort());

	return setNonBlock();
}

bool Socket::connect(const NetAddr* addr)
{
    if (addr == NULL) return false;

	_fd = socket(addr->getFamily(), SOCK_STREAM, 0);
	if (_fd<0)
	{
		XERR("[Socket],connect() %s:%lld socket failed %p", addr->getIP().c_str(), addr->getPort(), this);
		return false;
	}
    _addr = *addr;

    if (!setSockOpt())
        return false;

	INT ret = ::connect(_fd, (const sockaddr*)(addr->getSockAddr()), addr->getSockAddrLen());
	if (0!=ret)
	{
		XERR("[Socket],connect() %s:%lld failed with error %d %p", addr->getIP().c_str(), addr->getPort(), ret, this);
		return false;
	}

	XLOG("[Socket],%s,connect,%s:%lld,%u", _np->name(), addr->getIP().c_str(), addr->getPort(), _fd);
	return setNonBlock();
}

void Socket::close()
{
	if (valid())
	{
		SAFE_CLOSE_SOCKET(_fd, _np->name());
		_fd = -1;
	}
}

void Socket::shutdown(int how)
{
	if (valid())
	{
		XDBG("[Socket],shutdown %s, %d", _np->name(), _fd);
		::shutdown(_fd, how);
	}
}

bool Socket::setNonBlock()
{
	int flags = fcntl(_fd, F_GETFL, 0);
    flags |= O_NONBLOCK;
    if(-1 == fcntl(_fd, F_SETFL, flags))
	{
		XERR("[Socket],setNonBlock failed %s, %d", _np->name(), _fd);
        return false;
	}
    return true;
}

bool Socket::setSockOpt()
{
    int ret = -1;
	int nRecvBuf = MAX_BUFSIZE*2;
	ret = setsockopt(_fd,SOL_SOCKET,SO_RCVBUF,(const char*)&nRecvBuf,sizeof(int));
    if (ret == -1)
    {
        XERR("[Socket],%s,%d,设置 SO_RCVBUF 失败,%d,%s", _np->name(), _fd, errno, strerror(errno));
        return false;
    }

	int nSendBuf = MAX_BUFSIZE*2;
	ret = setsockopt(_fd,SOL_SOCKET,SO_SNDBUF,(const char*)&nSendBuf,sizeof(int));
    if (ret == -1)
    {
        XERR("[Socket],%s,%d,设置 SO_SNDBUF 失败,%d,%s", _np->name(), _fd, errno, strerror(errno));
        return false;
    }

    int flag = 1;
    ret = setsockopt(_fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(flag) );
    if (ret == -1)
    {
        XERR("[Socket],%s,%d,设置 TCP_NODELAY 失败,%d,%s", _np->name(), _fd, errno, strerror(errno));
        return false;
    }
    return true;
}

bool Socket::readToBuf()
{
	bool final_ret = true;
	while (1)
	{
		if (_read_buffer.GetLeft()<MAX_BUFSIZE)
		{
			_read_buffer.Resize(_read_buffer.buffer_size()+MAX_BUFSIZE);
		}

		int ret = ::recv(_fd, _read_buffer.GetBufOffset(), MAX_BUFSIZE, 0);
		if (ret<0)
		{
			if ((errno!=EAGAIN) && (errno!=EWOULDBLOCK) && (errno!=EINTR))
			{
				XERR("[SOCKET],%s,接收错误,fd:%d, ret:%d, errno:%u,%s", _np->name(), _fd, ret, errno, strerror(errno));
				final_ret = false;
			}
			break;
		}
		else if (0==ret)//peer shutdown
		{
			final_ret = false;
			XERR("[SOCKET],%s,接收错误,fd:%d, errno:%u,%s", _np->name(), _fd, errno, strerror(errno));
			break;
		}
		else
		{
			_read_buffer.Put(ret);
#ifdef _WUWENJUAN_DEBUG
      //XLOG("[SOCKET],fd:%u,recv:%u",_fd,ret);
#endif
		}
	}

	return final_ret;
}

int Socket::sendCmd()
{
	if (!valid()) return -1;

	compressAll();
//	ScopeWriteLock swl(_write_critical);

	int final_ret = 0;
	int all = _write_buffer.buffer_offset();
	while (all)
	{
		int realsend = std::min(all, MAX_BUFSIZE*2);
		int ret = ::send(_fd, _write_buffer.GetBufBegin(), realsend, 0);
		if (ret>0)
		{
			_write_buffer.Pop(ret);
			all = _write_buffer.buffer_offset();
#ifdef _WUWENJUAN_DEBUG
      //XLOG("[SOCKET],fd:%d,send:%u",_fd,ret);
#endif
		}
		else if (ret==0)
		{
			final_ret = _write_buffer.buffer_offset();
			XERR("[SOCKET],%s,发送异常,fd:%d,ret:%d,real:%d",_np->name(), _fd, ret, realsend);
			break;
		}
		else
		{
			if(errno != EWOULDBLOCK && errno != EINTR && errno != EAGAIN)
			{
				XERR("[SOCKET],%s,发送错误,fd:%d,ret:%d,real:%d,errno:%u,%s", _np->name(), _fd, ret, realsend, errno, strerror(errno));
				final_ret = -1;
				_write_buffer.Pop(all);
			}
            else
			    XERR("[SOCKET],%s,发送异常,fd:%d,ret:%d,real:%d,errno:%u,%s", _np->name(), _fd, ret, realsend, errno, strerror(errno));
			break;
		}
        usleep(500);
	}

	return final_ret;
}

int Socket::sendJson()
{
	if (!valid()) return -1;
    
	swapBuffer();
//	ScopeWriteLock swl(_write_critical);

	int final_ret = 0;
	int all = _write_buffer.buffer_offset();

	while (all)
	{
		int realsend = std::min(all, MAX_BUFSIZE*2);
		int ret = ::send(_fd, _write_buffer.GetBufBegin(), realsend, 0);
		if (ret>0)
		{
			_write_buffer.Pop(ret);
			all = _write_buffer.buffer_offset();
		}
		else if (ret==0)
		{
			final_ret = _write_buffer.buffer_offset();
			XERR("[SOCKET],%s,发送异常,fd:%d,ret:%d,real:%d",_np->name(), _fd, ret, realsend);
			break;
		}
		else
		{
			if(errno != EWOULDBLOCK && errno != EINTR && errno != EAGAIN)
			{
				XERR("[SOCKET],%s,发送错误,fd:%d,ret:%d,real:%d,errno:%u,%s", _np->name(), _fd, ret, realsend, errno, strerror(errno));
				final_ret = -1;
				_write_buffer.Pop(all);
			}
            else
			    XERR("[SOCKET],%s,发送异常,fd:%d,ret:%d,real:%d,errno:%u,%s", _np->name(), _fd, ret, realsend, errno, strerror(errno));
			break;
		}
	}

	return final_ret;
}

bool Socket::sendFlashPolicy()
{
	char policy[]="<?xml version=\"1.0\"?> <cross-domain-policy> <allow-access-from domain=\"*\" to-ports=\"*\" /> </cross-domain-policy>";
	int ret = 0;
	{
//		ScopeWriteLock swl(_write_critical);
		ret = ::send(_fd,(const char*)policy,sizeof(policy),0);
	}
	if(ret!=(int)sizeof(policy))
	{
		XERR("[SOCKET],fd:%d,send flashPolicy fail,errno:%u,%s,ret:%d",_fd,errno, strerror(errno),ret);
		return false;
	}
	else
	{
		shutdown(SHUT_RDWR);
		return true;
	}
}

bool Socket::sendCmd(const void *data, DWORD len)
{
  if (!valid()) return false;

  ScopeWriteLock swl(_cmd_write_critical);
  _cmd_write_buffer.Put(data,len);
  //XDBG("[SOCKET],fd:%d,send len:%u",_fd,len);
  return true;
}


bool Socket::sendJson(const void *data, WORD len)
{
  if (!valid()) return false;

  ScopeWriteLock swl(_cmd_write_critical);
  _cmd_write_buffer.Put(data,len);
  //XDBG("[SOCKET],fd:%d,send len:%u",_fd,len);
  return true;
}

bool Socket::writeToBuf(void *data, DWORD len)
{
    while (_write_buffer.GetLeft() < len)
        _write_buffer.Resize(_write_buffer.buffer_size() + MAX_BUFSIZE * (((len - _write_buffer.GetLeft()) / MAX_BUFSIZE) + 1));

	DWORD real_size = 0;

//	ScopeWriteLock swl(_write_critical);
	real_size = _write_buffer.Put(data, len);

	return (real_size==len);
}

bool Socket::getCmd(BYTE *&cmd, DWORD &len)
{
	//return cmdQueue.getCmd(cmd, len);

	cmd = 0;
	len = 0;

	DWORD used = _read_buffer.buffer_offset();
	if (used<=0) return false;

	if(*((BYTE*)_read_buffer.GetBufBegin())==147 || *((BYTE*)_read_buffer.GetBufBegin())==148)
	{
        int tagLen = 0;
        BYTE tagByte = ((BYTE*)_read_buffer.GetBufBegin())[1];
        if(tagByte / 16 >= 10 && tagByte / 16 <= 15)
        {
            tagLen = (tagByte / 16 - 10 ) * 16 + tagByte % 16;
        }

        if(tagLen != 0)
        {
            if(tagLen > 99) tagLen = 99;
            char tag[100];
            memset(tag, 0, 100);
            const BYTE * tagName = &(((BYTE*)_read_buffer.GetBufBegin())[2]);
            memcpy(tag, tagName, tagLen);

            if(!_np->connect(std::string(tag)))
            {
                XERR("[Socket], sockid:%d,找不到Tag:%s服务器配置", get_fd(), tag);
            }
            else
            {
                XLOG("[Socket], sockid:%d, tag:%s", get_fd(), tag);
            }
        }
        
        /*char printout[255];
        memset(printout, 0, 255);
        DWORD outlen = 254 >  _read_buffer.buffer_offset()? _read_buffer.buffer_offset():254;
        memcpy(printout, (BYTE*)_read_buffer.GetBufBegin(), outlen);
        XLOG("[Socket], 收到的前255字符是:%s",printout);
        */
	}
	XDBG("收到数据包的前两个字符分别是:%d,%d.", (int)(((BYTE*)_read_buffer.GetBufBegin())[0]), (int)(((BYTE*)_read_buffer.GetBufBegin())[1]));
    cmd = (BYTE *)_read_buffer.GetBufBegin();
	len = _read_buffer.buffer_offset();
	unCompCmdRealSize = len;
#ifdef _WUWENJUAN_DEBUG
  //XLOG("[SOCKET],fd:%u,recv cmd len:%u",_fd,len);
#endif
	return true;
}

bool Socket::popCmd()
{
	if (unCompCmdRealSize > 0 && unCompCmdRealSize <= _read_buffer.buffer_offset())
	{
		if (_read_buffer.Pop(unCompCmdRealSize))
		{
			unCompCmdRealSize = 0;
			return true;
		}
	}

	return false;
}

void Socket::compressAll()
{
	{
		_tmp_cmd_write_buffer.Reset();
		ScopeWriteLock swl(_cmd_write_critical);
		_tmp_cmd_write_buffer.Copy(_cmd_write_buffer);
		_cmd_write_buffer.Reset();
	}

	while(_tmp_cmd_write_buffer.buffer_offset()>0)
	{
        {
            writeToBuf(_tmp_cmd_write_buffer.GetBufBegin(), _tmp_cmd_write_buffer.buffer_offset());
            _tmp_cmd_write_buffer.Reset();
        }
	}
}

void Socket::swapBuffer()
{
	{
		_tmp_cmd_write_buffer.Reset();
		ScopeWriteLock swl(_cmd_write_critical);
		_tmp_cmd_write_buffer.Copy(_cmd_write_buffer);
		_cmd_write_buffer.Reset();
	}

    UINT buffSize = _tmp_cmd_write_buffer.buffer_offset();
	if(buffSize>0)
	{
		if (!writeToBuf(_tmp_cmd_write_buffer.GetBufBegin(), buffSize))
		{
			XLOG("[Socket],%s,fd:%d,push cmd error", _np->name(), _fd);
		}
		_tmp_cmd_write_buffer.Pop(buffSize);
	}
}
