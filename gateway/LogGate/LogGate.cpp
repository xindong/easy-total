#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <string>
#include <list>
#include "LogGate.h"
#include "xXMLParser.h"
#include "xTools.h"
#include "Socket.h"
#include "NetProcessor.h"
#include "ServerConfig.h"
#include "log4cxxconfigurator.h"
#include <log4cxx/ndc.h>

LogGate *thisServer = 0;

void handle_pipe(int s)
{
    XLOG("[handle_pipe]");
}

LogGate::LogGate(const char* name, std::string ip_, WORD p): listener(this), ip(ip_), port(p)
{
    bzero(serverName, sizeof(serverName));
    strncpy(serverName, name, MAX_NAMESIZE);

    setServerState(SERVER_CREATE);

    struct sigaction action;
    action.sa_handler = handle_pipe;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;
    sigaction(SIGPIPE, &action, NULL);
}

LogGate::~LogGate()
{
}

/**
 * @brief 停机过程控制
 * 先关闭监听的client端Socket
 * 在监听端未全部关闭以前，保持与主动建立的socket服务端的连接关系
 *
 * @return 
 */
bool LogGate::v_stop()
{
	for (auto it = _serverList.begin(); it != _serverList.end(); it++)
	{
        if (it->second)
        {
            NetProcessor* np = it->second;
            it->second = NULL;
            addToCloseList_th(np);
        }
    }
    XLOG("[%s],v_stop:%p", getServerName(), this);
    return true;
}

void LogGate::v_final()
{
    XLOG("[LogGate],_close_list");
    _close_iter tmp_close;
    NetProcessor* clo = 0;
    {
        ScopeWriteLock swl(_close_critical);
        for (_close_iter it = _close_list.begin(); it != _close_list.end(); )
        {
            tmp_close = it++;

            clo = tmp_close->first;
			clo->disconnect();

            XLOG("[%s]%s delete %p", serverName, clo->name(), clo);
            SAFE_DELETE(clo);
        }
        _close_list.clear();
    }

    xXMLParser::clearSystem();
	listener.thread_stop();

    _serverList.clear();

    XLOG("[LogGate],final");
}

bool LogGate::listen()
{
    if (listener.getListenPort() == 0)
    {
        XERR("[%s],监听端口错误", getServerName());
        return false;
    }

    if (!listener.thread_start())
    {
        XERR("[%s]start listener failed", getServerName());
        return false;
    }
    XLOG("[%s],开始监听,%u", getServerName(), listener.getListenPort());
    return true;
}

void LogGate::setListenAddr(const NetAddr* addr) 
{ 
    listener.listen_addr = *addr;
}

bool LogGate::startListen()
{
    if (!listen())
    {
        server_stop();
        return false;
    }
    return true;
}

void LogGate::setServerState(ServerState s)
{
    server_state = s;
    switch (server_state)
    {
        case SERVER_CREATE:
            {
                XLOG("[%s],创建成功", serverName);
            }
            break;
        case SERVER_INIT:	//init before connect
            {
                XLOG("[%s],准备初始化数据", serverName);
            }
            break;
        case SERVER_CONNECT:
            {
                XLOG("[%s],开始网络连接", serverName);
            }
            break;
        case SERVER_RUN://all server connected, do work
            {
                XLOG("[%s],初始化完毕，开始运行", serverName);
            }
            break;
        case SERVER_SAVE:
            {
                XLOG("[%s],保存数据，即将终止", serverName);
            }
            break;
        case SERVER_STOP://will server_stop
            {
                XLOG("[%s],主循环结束，即将终止", serverName);
            }
            break;
        case SERVER_FINISH://all done
            {
                XLOG("[%s],进程终止", serverName);
            }
            break;
        default:
            break;
    }
}

//主线程和子线程公用
bool LogGate::addToCloseList_th(NetProcessor* np)
{
    if (!np) return false;

    bool ret = false;
    ScopeWriteLock swl(_close_critical);
    if (_close_list.find(np) != _close_list.end())
    {
        XERR("[关闭连接],重复关闭,%s,%p", np->name(), np);
        return false;
    }
    else
    {
        /*np->delClientEpoll();
        np->delAllServerEpoll();
        np->disconnect();
        */
        np->thread->thread_setState(xThread::THREAD_STOP);
        _close_list[np] = time(0);
        XDBG("[关闭连接],%s,%p", np->name(), np);
    }

    return ret;
}

void LogGate::select_th(int epfd, int sock, epoll_event events[], NetProcessor* np)
{
    if (!epfd) return;

    bzero(events, MAX_SERVER_EVENT * sizeof(epoll_event));
    int nfds = epoll_wait(epfd, events, MAX_SERVER_EVENT, EPOLL_WAIT_TIMEOUT);
    for (int i = 0; i < nfds; ++i)
    {
        if ((events[i].data.fd == sock))
        {
            BYTE caddr[256];
            int addrlen = sizeof(caddr);
            bzero(&caddr, sizeof(caddr));
            int cfd = ::accept(sock, (struct sockaddr*)&caddr, (socklen_t*)&addrlen);
            if (-1 == cfd)
            {
                XERR("%u,%u,accept 出错,errno:%u,%s.", epfd, sock, errno, strerror(errno));
                sleep(1);
            }
            else
            {
                accept(cfd, (const sockaddr*)caddr, addrlen, epfd);
            }
        }
        else
        {
            int sockid = events[i].data.fd;
            if (!np) continue;

            if (events[i].events & EPOLLERR)
            {
                XLOG("[%s]连接错误 %s %p", serverName, np->name(), np);
                np->set_np_state(NP_DISCONNECT);
                addToCloseList_th(np);
                continue;
            }
            else
            {
                if (events[i].events & EPOLLOUT)
                {
                    int ret = -1;
                    if(sockid == np->getClientSocket().get_fd())
                    {
                        ret = np->realSendClientCmd();
                    }
                    else
                    {
                        ret = np->realSendServerCmd(sockid);
                    }
                    if (-1 == ret)
                    {
                        XDBG("[%s],发送错误 %s %p, sockid:%d", serverName, np->name(), np, sockid);
                        np->set_np_state(NP_DISCONNECT);
                        addToCloseList_th(np);
                        continue;
                    }
                    else if (ret > 0)
                    {
                        if(sockid == np->getClientSocket().get_fd())
                        {
                            np->addClientEpoll();
                        }
                        else
                        {
                            np->addServerEpoll(sockid);
                        }
                    }
                }
                if (events[i].events & EPOLLIN)
                {
                    bool readret = false;
                    if(sockid == np->getClientSocket().get_fd())
                    {
                        readret = np->readCmdFromClientSocket();
                    }
                    else
                    {
                        readret = np->readCmdFromServerSocket(sockid);
                    }
                    if(!readret)
                    {
                        XDBG("[%s]读取错误 %s %p, sockid:%d", serverName, np->name(), np, sockid);
                        np->set_np_state(NP_DISCONNECT);
                        addToCloseList_th(np);
                        continue;
                    }

                    unsigned char* cmd = NULL;
                    unsigned int cmdLen;
                    if(sockid == np->getClientSocket().get_fd())
                    {
                        while (np->getCmdFromClientSocketBuf(cmd, cmdLen))
                        {
                            np->sendCmdToServer(cmd, cmdLen);
                            np->popCmdFromClientSocketBuf();
                        }
                    }
                    else
                    {
                        while (np->getCmdFromServerSocketBuf(sockid, cmd, cmdLen))
                        {
                            np->sendCmdToClient(cmd, cmdLen);
                            np->popCmdFromServerSocketBuf(sockid);
                        }
                    }
                }
            }
        }
    }
}

bool LogGate::callback()
{
    xTime frameTimer;

    QWORD curUSec = xTime::getCurUSec();
    v_timetick(curUSec);

    if (!v_callback())
        return false;

    QWORD _e = frameTimer.uElapse();

    if (_e < getFrameElapse())
        usleep(getFrameElapse() - _e);
    else if (_e > WARN_SERVER_FRAME_TIME)
        XLOG("[%s],帧耗时 %llu 微秒", serverName, _e);

    return true;
}

void LogGate::run()
{
#define FINAL_RETURN { final(); return; }
    // daemon(1,1);
    setServerState(SERVER_INIT);
    xXMLParser::initSystem();
    ServerConfig::getMe().loadConfig();
    srand(xTime::getCurUSec());
    setListenAddr(ServerConfig::getMe().getLocalAddr());

    while (callback());
    setServerState(SERVER_STOP);

    v_final();
    setServerState(SERVER_FINISH);
#undef FINAL_RETURN
}

//主线程
bool LogGate::v_callback()
{
    switch (getServerState())
    {
        case SERVER_CREATE:
            {
                return true;
            }
            break;
        case SERVER_INIT:
            {
                if (!initAction())
                {
                    server_stop();
                }
                else
                {
                    setServerState(SERVER_CONNECT);
                }
                return true;
            }
            break;
        case SERVER_CONNECT:
            {
                if (connectAction())
                {
                    initAfterConnect();
                    setServerState(SERVER_RUN);
                }
                return true;
            }
            break;
        case SERVER_RUN:
            {
                if (!runAction())
                {
                    initAfterConnect();
                    XERR("[%s]runAction err", getServerName());
                    server_stop();
                }
            }
            break;
        case SERVER_SAVE:
            {
                return true;
            }
            break;
        case SERVER_STOP:
            {
                if (v_stop())
                {
                    //退出主循环
                    return false;
                }
                else
                {
				    XLOG("[%s] server stop", getServerName());
                    return true;
                }
            }
            break;
        case SERVER_FINISH:
            {
                return false;
            }
            break;
        default:
            break;
    }
    return true;
}

bool LogGate::initAction()
{
    return true;
}

bool LogGate::connectAction()
{
    if (!startListen()) return false;
    std::cout << "[" << getServerName() << "],可以开始游戏了" << std::endl;
    return true;
}

bool LogGate::runAction()
{
    return true;
}

bool LogGate::accept(int sockfd, const sockaddr* addr, DWORD addr_len, int epfd)
{
    NetProcessor* task = newTask();
    task->accept(sockfd, addr, addr_len);

    task->thread = NEW TaskThread(this, task);
    if (!task->thread->thread_start())
    {
        XERR("[%s]start task failed", getServerName());
        SAFE_DELETE(task);
        return false;
    }
    task->addClientEpoll(task->thread->listen_epfd);
    _serverList[getProcessorID()] = task;
    XDBG("[%s]%s:%d connect, new task:%p, sockid:%d, add verify list", getServerName(), task->getClientAddr().getIP().c_str(), task->getClientAddr().getPort(), task, sockfd);

    return true;
}

NetProcessor* LogGate::newTask()
{
    return NEW NetProcessor("Task");;
}

void LogGate::v_CloseNp(NetProcessor* np)	//主线程
{
    for (auto it = _serverList.begin(); it != _serverList.end(); it++)
    {
        if (it->second == np)
        {
            XDBG("[LogGate],关闭连接,NetProcessor名称:%s", it->second->name());
            it->second = NULL;
            break;
        }
    }
}

#include <signal.h>
void kill_handler(int s)
{
    if (thisServer && thisServer->getServerState() <  LogGate::SERVER_STOP)
        thisServer->server_stop();
}

int main(int argc,char*argv[])
{
    signal(SIGTERM, kill_handler);
    signal(SIGSTOP, kill_handler);
    signal(SIGINT, kill_handler);

    int oc = -1;
    std::string configFile;
    std::string servername;
    bool isdaemon = false;
    std::string ip;
    WORD port = 0;
    while((oc = getopt(argc, argv, "dc:n:p:i:")) != -1)
    {
        switch(oc)
        {
            case 'i':
                {
                    ip = optarg;
                }
                break;
            case 'p':
                {
                    port = atoi(optarg);
                }
                break;
            case 'd':
                {
                    isdaemon = true;
                }
                break;
            case 'c':
                {
                    configFile = optarg;
                }
                break;
            case 'n':
                {
                    servername = optarg;
                }
                break;
            case '?':
                break;
        }
    }
    if(isdaemon)
    {
        int ret = daemon(1, 1);
        if (ret == -1) printf("设置daemon出错");
    }
    if( servername.empty())
    {
        printf("命令行参数错误，请指定 -n ServerName");
        return -1;
    }
    if(port == 0)
    {
        printf("命令行参数错误，请指定 -n Port");
        return -1;
    }

    char servername_env[128] = {0};
    sprintf(servername_env, "servername=%s", servername.c_str());
    putenv(servername_env);

    const std::string logconfig = "logConfig/LogGate.xml";
    const long delay = 5000;
    setlocale(LC_ALL, "");
    Log4cxxConfigurator::XmlWatchdog dog(logconfig, delay);
    //NDC用于在日志中标记不同的线程...
    log4cxx::NDC ndc("");

    XLOG("------------------------------------------");
    XLOG("server start...");
    thisServer = NEW LogGate(servername.c_str(), ip, port);

    if (thisServer)
    {
        thisServer->run();

        SAFE_DELETE(thisServer);
    }
    else
        XERR("命令行参数错误 请指定 -n ServerName -p Port");
    return 0;
}
