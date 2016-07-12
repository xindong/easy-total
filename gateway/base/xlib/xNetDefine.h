#pragma once

#include "xDefine.h"
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define INET_IPV4

//地址协议族
#ifdef INET_IPV4
#define SA_FAMILY AF_INET
#define IP_LEN (INET_ADDRSTRLEN+1)
#else
#define SA_FAMILY AF_INET6
#define IP_LEN (INET6_ADDRSTRLEN+1)
#endif

struct IP
{
    union IP_NET
    {
        in_addr ipv4_net;
        in6_addr ipv6_net;
    };

    public:
        IP()
        {
            bzero(&ip_net, sizeof(ip_net));
            sa_family = 0;
        }
        IP(sa_family_t family) 
        {
            sa_family = family;
            bzero(&ip_net, sizeof(ip_net));
        }
        IP(sa_family_t family, const std::string& _ip)
        {
            sa_family = family;
            setIP(_ip);
        }

        void setFamily(sa_family_t fa) {sa_family = fa;}
        sa_family_t getFamily() const {return sa_family;}

        std::string getIP()const {return ip_host;}
        const void* getIPNet()const 
        {
            if (sa_family == AF_INET)
            {
                return &ip_net.ipv4_net;
            }
            else if (sa_family == AF_INET6)
            {
                return &ip_net.ipv6_net;
            }
            else
                return NULL;
        }
        void setIPNet(const void* _ip)
        {
            if (_ip == NULL) return;
            if (sa_family == AF_INET)
            {
                memcpy(&(ip_net.ipv4_net), &_ip, sizeof(in_addr));
            }
            else if (sa_family == AF_INET6)
            {
                memcpy(&(ip_net.ipv6_net), &_ip, sizeof(in6_addr)); 
            }
            else
            {
                return;
            }
            char ipstr[64];
            bzero(ipstr, sizeof(ipstr));
            inet_ntop(sa_family, _ip, ipstr, sizeof(ipstr));
            ip_host = ipstr;
        }
        void setIP(const std::string& _ip)
        {
            if (!_ip.empty())
            {
                ip_host = _ip;
                inet_pton(sa_family, ip_host.c_str(), &ip_net);
            }
        }

        void clear()
        {
            ip_host.clear();
            bzero(&ip_net, sizeof(ip_net));
        }

        IP& operator = (const IP& _ip)
        {
            this->ip_host = _ip.ip_host;
            memcpy(&(this->ip_net), &(_ip.ip_net), sizeof(this->ip_net));
            this->sa_family = _ip.sa_family;
            return *this;
        }

    private:
        std::string ip_host;
        IP_NET ip_net;
        sa_family_t sa_family; //协议类型
};

struct Port
{
    public:
        Port() 
        {
            setPort(0);
        }
        Port(WORD p)
        {
            setPort(p);
        }
        void setPort(WORD _p) 
        {
            if (_p)
            {
                port_host = _p;
                port_net = htons(port_host);
            }
            else
            {
                port_host = 0;
                port_net = 0;
            }
        }
        void setPortNet(WORD _p)
        {
            if (_p)
            {
                port_net = _p;
                port_host = ntohs(port_net);
            }
            else
            {
                port_net = 0;
                port_host = 0;
            }
        }
        WORD getPort()const {return port_host;}
        WORD getPortNet()const {return port_net;}
        void clear()
        {
            port_host = 0;
            port_net = 0;
        }
    private:
        WORD port_host;
        WORD port_net;
};

struct NetAddr
{
    public:
        NetAddr() {}
        NetAddr(sa_family_t family) : m_ip(family) {}
        NetAddr(const NetAddr& naddr)
        {
            this->m_ip = naddr.m_ip;
            this->m_port = naddr.m_port;
        }

        sa_family_t getFamily() const {return m_ip.getFamily();}

        void setIP(const std::string& _ip) {m_ip.setIP(_ip);}
        std::string getIP() const {return m_ip.getIP();}

        void setPort(WORD _port) {m_port.setPort(_port);}
        void setPortNet(WORD _port) {m_port.setPortNet(_port);}
        WORD getPort()const {return m_port.getPort();}
        WORD getPortNet()const {return m_port.getPortNet();}

        void setIPAndPort(const sockaddr* _addr, DWORD len)
        {
            if (_addr == NULL) return;
            if (_addr->sa_family == AF_INET)
            {
                if (len < sizeof(sockaddr_in)) return;
                m_ip.setFamily(_addr->sa_family);  
                const sockaddr_in* ta = (const sockaddr_in*)_addr;
                m_ip.setIPNet(&ta->sin_addr);
                m_port.setPortNet(ta->sin_port);
            }
            else if (_addr->sa_family == AF_INET6)
            {
                if (len < sizeof(sockaddr_in6)) return;
                m_ip.setFamily(_addr->sa_family);  
                const sockaddr_in6* ta = (const sockaddr_in6*)_addr;
                m_ip.setIPNet(&ta->sin6_addr);
                m_port.setPortNet(ta->sin6_port);
            }
            else
                return;
        }

        const sockaddr* getSockAddr() const
        {
            if (getFamily() == AF_INET)
            {
                static sockaddr_in addr;
                addr.sin_family = getFamily();
                addr.sin_port = m_port.getPortNet();
                memcpy(&addr.sin_addr, m_ip.getIPNet(), sizeof(addr.sin_addr));
                return (const sockaddr*)&addr;
            }
            else if (getFamily() == AF_INET6)
            {
                static sockaddr_in6 addr;
                addr.sin6_family = getFamily();
                addr.sin6_port = m_port.getPortNet();
                memcpy(&addr.sin6_addr, m_ip.getIPNet(), sizeof(addr.sin6_addr));
                return (const sockaddr*)&addr;
            }
            else
                return NULL;
        }
        DWORD getSockAddrLen()const
        {
            if (getFamily() == AF_INET)
                return sizeof(sockaddr_in);
            else if(getFamily() == AF_INET6)
                return sizeof(sockaddr_in6);
            else
                return 0;
        }

        void clear()
        {
            m_ip.clear();
            m_port.clear();
        }

        NetAddr& operator = (const NetAddr& addr)
        {
            this->m_ip = addr.m_ip;
            this->m_port = addr.m_port;
            return *this;
        }

    protected:
        IP m_ip;
        Port m_port;
};

struct DoubleNetAddr
{
    public:
        DoubleNetAddr() {}
        DoubleNetAddr(sa_family_t family)
            : m_addr(family), m_extaddr(family)
        {
        }
        DoubleNetAddr(const DoubleNetAddr& dnaddr)
        {
            this->m_addr = dnaddr.m_addr;
            this->m_extaddr = dnaddr.m_extaddr;
        }

        const NetAddr* getAddr() const {return &m_addr;}
        const NetAddr* getExtAddr() const {return &m_extaddr;}

        sa_family_t getFamily() const {return m_addr.getFamily();}

        void setIP(const std::string& _ip){m_addr.setIP(_ip);}
        std::string getIP()const {return m_addr.getIP();}
        void setExtIP(const std::string& _ip) {m_extaddr.setIP(_ip);}
        std::string getExtIP() const {return m_extaddr.getIP();}

        void setPort(WORD _port) {m_addr.setPort(_port);}
        void setExtPort(WORD _port) {m_extaddr.setPort(_port);}
        WORD getPort() const {return m_addr.getPort();}
        WORD getPortNet() const {return m_addr.getPortNet();}
        WORD getExtPort() const {return m_extaddr.getPort();}
        WORD getExtPortNet() const {return m_extaddr.getPortNet();}

        DoubleNetAddr& operator = (const DoubleNetAddr& daddr)
        {
            this->m_addr = daddr.m_addr;
            this->m_extaddr = daddr.m_extaddr;
            return *this;
        }

    private:
        NetAddr m_addr;     //内网地址
        NetAddr m_extaddr;  //外网地址
};

