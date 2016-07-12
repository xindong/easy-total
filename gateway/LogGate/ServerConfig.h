#include "xSingleton.h"
#include <string>
#include <map>
#include "xNetDefine.h"

struct ServerInfo
{
    public:
        ServerInfo() : addr(AF_INET)
        {
            tag = "";
        }
        std::string tag;
        DoubleNetAddr addr;
};

class ServerConfig : public xSingleton<ServerConfig>
{
    public:
        ServerConfig();
        ~ServerConfig();

        bool loadConfig();
        const NetAddr* getLocalAddr() { return localAddr.addr.getAddr(); }
        const NetAddr* getAddrByTag(std::string tag)
        {
            auto it = serverList.find(tag);
            if(it != serverList.end())
            {
                return it->second.addr.getAddr();
            }
            return NULL;
        }

    private:
        ServerInfo localAddr;
        std::map<std::string, ServerInfo> serverList;
};
