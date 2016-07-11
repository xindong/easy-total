#include "NetProcessor.h"
#include "ServerConfig.h"

NetProcessor::NetProcessor(const char *n)
:clientSock(this)
{
	set_np_state(NP_CREATE);
	set_name(n);
	thread = NULL;
    cur_server_sock = NULL;
}

NetProcessor::~NetProcessor()
{
	if (thread)
	{
		thread->thread_stop();
		SAFE_DELETE(thread);
        thread = NULL;
	}
}

void NetProcessor::disconnect()
{
	clientSock.close();
	XDBG("[Socket],%s disconnect %p", name(), this);
    for(auto it = server_sockets.begin(); it != server_sockets.end(); it++)
    {
        if(it->second)
        {
            it->second->close();
            SAFE_DELETE(it->second);
            it->second = NULL;
        }
    }
	set_np_state(NP_CLOSE);
}

bool NetProcessor::sendCmdToClient(const void *cmd, unsigned int len)
{
	if (np_state() == NP_CLOSE ||  NP_DISCONNECT == np_state()) return false;
	if (!cmd || !len) return false;

	if (!clientSock.sendCmd(cmd, len))
	{
		XERR("sendCmd failed %p", this);
		return false;
	}
	else
	{
		addClientEpoll();
	}
	return true;
}

bool NetProcessor::sendCmdToServer(const void *cmd, unsigned int len)
{   
    if (np_state() == NP_CLOSE ||  NP_DISCONNECT == np_state()) return false;
    if (!cmd || !len) return false;

    if (!cur_server_sock || !cur_server_sock->sendCmd(cmd, len))
    {   
        XERR("sendCmd failed %p", this);
        return false;
    }
    else
    {   
        addServerEpoll(cur_server_sock->get_fd());
    }
    return true;
}

bool NetProcessor::connect(std::string tag)
{
    bool ret = false;
    Socket* serverSocket = NULL;
    serverSocket = getServerSocket(tag);
    if(serverSocket)
    {
        cur_server_sock = serverSocket;
        ret = true;
        return ret;
    }
    const NetAddr* tag_addr = ServerConfig::getMe().getAddrByTag(tag);
    if(tag_addr != NULL)
    {
        serverSocket = NEW Socket(this);
        if(serverSocket->connect(tag_addr))
        {
            serverSocket->addEpoll(thread->listen_epfd);
            tag_sockid_list[tag] = serverSocket->get_fd();
            server_sockets[serverSocket->get_fd()] = serverSocket;
            cur_server_sock = serverSocket;
            ret = true;
        }
    }
    return ret;
}
