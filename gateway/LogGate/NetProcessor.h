#pragma once
#include "Socket.h"
#include "TaskThread.h"
#include "xTime.h"
#include "xEntry.h"

enum NPState
{
	NP_CREATE = 0,
	NP_VERIFIED,
	NP_ESTABLISH,
	NP_DISCONNECT,
	NP_CLOSE,
};
class TaskThread;
class NetProcessor : public xEntry
{
	friend class LogGate;
	public:
		NetProcessor(const char *n);
		virtual ~NetProcessor();

	/****************************************************************/
	/*                             TCP                              */
	/****************************************************************/
	public:
		void addClientEpoll(int fd)
		{
			clientSock.addEpoll(fd);
		}
		void addClientEpoll()
		{
			clientSock.addEpoll();
		}
		void delClientEpoll()
		{
			clientSock.delEpoll();
		}

        void addServerEpoll(std::string tag, int fd)
        {
            auto it = tag_sockid_list.find(tag);
            if(it != tag_sockid_list.end())
            {
                addServerEpoll(it->second, fd);
            }
        }

        void addServerEpoll(int sockid, int fd)
        {
            auto it = server_sockets.find(sockid);
            if(it != server_sockets.end())
            {
                it->second->addEpoll(fd);
            }
        }

        void addServerEpoll(std::string tag)
        {
            auto it = tag_sockid_list.find(tag);
            if(it != tag_sockid_list.end())
            {
                addServerEpoll(it->second);
            }
        }

        void addServerEpoll(int sockid)
        {
            auto it = server_sockets.find(sockid);
            if(it != server_sockets.end())
            {
                it->second->addEpoll();
            }
        }

        void delServerEpoll(std::string tag)
        {
            auto it = tag_sockid_list.find(tag);
            if(it != tag_sockid_list.end())
            {
                delServerEpoll(it->second);
            }
        }

        void delServerEpoll(int sockid)
        {
            auto it = server_sockets.find(sockid);
            if(it != server_sockets.end())
            {
                it->second->delEpoll();
            }
        }

        void delAllServerEpoll()
        {
            for(auto it = server_sockets.begin(); it != server_sockets.end(); it++)
            {
                it->second->delEpoll();
            }
        }

	public:
		bool isValid()
		{
		  return clientSock.valid();
		}
		bool connect(std::string tag);
		bool accept(int sockfd, const sockaddr* addr, DWORD addr_len)
		{
			return clientSock.accept(sockfd, addr, addr_len);
		}
		void disconnect();

		//发送消息
		bool sendCmdToClient(const void *cmd, unsigned int len);
		bool sendCmdToServer(const void *cmd, unsigned int len);

		int realSendClientCmd()
		{
			return clientSock.sendCmd();
		}

        int realSendServerCmd(int sockid)
        {
            auto it = server_sockets.find(sockid);
            if(it != server_sockets.end())
            {
                return it->second->sendCmd();
            }
            return -1;
        }
		//接收消息
		bool readCmdFromClientSocket()
		{
			return clientSock.readToBuf();
		}
        
        bool readCmdFromServerSocket(int sockid)
        {
            auto it = server_sockets.find(sockid);
            if(it != server_sockets.end())
            {
                return it->second->readToBuf();
            }
            return false;
        }

		bool getCmdFromClientSocketBuf(unsigned char *&cmd, unsigned int &len)
		{
			return clientSock.getCmd(cmd, len);
		}
		bool getCmdFromServerSocketBuf(int sockid, unsigned char *&cmd, unsigned int &len)
		{
            auto it = server_sockets.find(sockid);
            if(it != server_sockets.end())
            {
			    return it->second->getCmd(cmd, len);
            }
            return false;
		}

		bool popCmdFromClientSocketBuf()
		{
			return clientSock.popCmd();
		}
        bool popCmdFromServerSocketBuf(int sockid)
        {   
            auto it = server_sockets.find(sockid);
            if(it != server_sockets.end())
            {   
                return it->second->popCmd();
            }
            return false;
        }
		const Socket& getClientSocket() const 
		{
			return clientSock;
		}
        const NetAddr& getClientAddr() const
        {
            return clientSock.getAddr();
        }

        Socket* getCurServerSock()
        {
            return cur_server_sock;
        }
        Socket* getServerSocket(std::string tag)
        {
            auto it = tag_sockid_list.find(tag);
            if(it != tag_sockid_list.end())
            {
                return getServerSocket(it->second);
            }
            return NULL;
        }
        Socket* getServerSocket(int sockid)
        {
            auto it = server_sockets.find(sockid);
            if(it != server_sockets.end())
            {
                return it->second;
            }
            return NULL;
        }
	protected:
		Socket clientSock;
        std::map<int, Socket*> server_sockets;
        std::map<std::string, int> tag_sockid_list;

        Socket* cur_server_sock;

	public:
		NPState np_state() { return np_state_; }
		void set_np_state(NPState np_state) { np_state_ = np_state; }
	private:
		NPState np_state_;

	public:
		TaskThread *thread;
};
