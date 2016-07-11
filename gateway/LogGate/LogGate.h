#pragma once
#include <netinet/in.h>
#include <sys/epoll.h>
#include <map>
#include "xDefine.h"
#include "xTime.h"
#include "xMutex.h"
#include "ListenThread.h"
#include "xNetDefine.h"
#include "NetProcessor.h"
class NetProcessor;

class LogGate
{
    public:
        LogGate(const char* name, std::string ip, WORD p);
        virtual ~LogGate();

        enum ServerState
        {
            SERVER_CREATE = 0,
            SERVER_INIT,  // connect other server
            SERVER_CONNECT,
            SERVER_RUN,  // all server connected, do work
            SERVER_SAVE,  // 保存数据
            SERVER_STOP,  // will server_stop
            SERVER_FINISH,  // all done
        };

        void server_stop() { setServerState(SERVER_STOP); }
        const char* getServerName() const { return serverName; }

        void run();
        void select_th(int epfd, int sock, epoll_event evs[], NetProcessor* np);

    protected:
        bool v_callback();
        DWORD getFrameElapse() const {return SERVER_FRAME_TIME;}
        bool initAction();
        bool connectAction();
        bool runAction();
        void initAfterConnect() {}

        NetProcessor* newTask();

        void v_final();
        bool v_stop();
        void v_timetick(QWORD usec);
        bool accept(int sockfd, const sockaddr* addr, DWORD addr_len, int epfd);
        bool callback();

    private:
        char serverName[MAX_NAMESIZE];  // server name


        /*************************************************************//**
         *                         进程状态管理
         ****************************************************************/
    public:
        ServerState getServerState() { return server_state; }
        void setServerState(ServerState s);

    private:
        volatile ServerState server_state;


        /*************************************************************//**
         *                        监听服务器进程间连接
         ****************************************************************/
    protected:
        bool listen();
        bool isListening() { return listener.start(); }
        int getServerEpfd() { return listener.listen_epfd; }
        int getEpfd() { return getServerEpfd(); }
        void setListenAddr(const NetAddr* addr);
        bool startListen();
    public:
        WORD getListenPort() { return port; }
        std::string getListenIP() { return ip; }

    private:
        ListenThread listener;
        std::string ip;
        WORD port;

        /*************************************************************//**
         *                        待关闭网络连接列表 
         ****************************************************************/
    public:
        virtual bool addToCloseList_th(NetProcessor* np);  // 添加到删除列表里面 等callback删除

    private:
        xRWLock _close_critical;
        std::map<NetProcessor*, time_t> _close_list;   // np:time 删除列表
        typedef std::map<NetProcessor*, time_t>::iterator _close_iter;

        /*************************************************************//**
         *                   已连接和待连接网络连接管理 
         ****************************************************************/
    public:
        QWORD getProcessorID()
        {
            if(processorID >= QWORD_MAX)
            {
                processorID = 0;
            }
            return processorID++;
        }
        void v_CloseNp(NetProcessor*);

    protected:
        std::map<QWORD, NetProcessor*> _serverList;
        QWORD processorID;
};

extern LogGate *thisServer;
