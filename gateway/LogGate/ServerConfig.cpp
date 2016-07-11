#include "ServerConfig.h"
#include "xXMLParser.h"
#include "log.h"
#include "LogGate.h"

ServerConfig::ServerConfig()
{
    serverList.clear();
}

ServerConfig::~ServerConfig()
{
}

bool ServerConfig::loadConfig()
{
    LOAD_CONFIG_HEAD("ServerList.xml","服务器列表配置");

    xmlNodePtr listNode = p.getChild(root,"ServerList");
    if(!listNode)
    {
        XERR("[务器列表配置]加载ServerList节点失败");
        return false;
    }
    serverList.clear();
    std::string tag;
    xmlNodePtr infoNode = p.getChild(listNode,"info");
    while(infoNode)
    {
        tag.clear();
        p.getPropStr(infoNode, "tag", tag);
        ServerInfo& info = serverList[tag];
        tag.clear();
        p.getPropStr(infoNode, "ip", tag);
        info.addr.setIP(tag);
        WORD port;
        p.getPropValue(infoNode, "port", &port);
        info.addr.setPort(port);
        infoNode = p.getNext(infoNode, "info");
    }

    localAddr.addr.setIP(thisServer->getListenIP());
    localAddr.addr.setPort(thisServer->getListenPort());
    
    XLOG("[服务器列表配置],加载成功");
    return true;
}
