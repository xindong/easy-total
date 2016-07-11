# Fluentd Forward 网关

此网关配合EasyTotal使用，可以将指定的tag转发到对应的服务器上，实现EasyTotal分布式部署的功能，本功能由心动游戏的连正权同学开发。


**配置说明**

1. ServerConfig.xml里面，要配本地监听地址ip，端口port，对应的进程名

2. Config/ServerList.xml里面，ServerList结点下面配置tag对应的名字和服务器地址信息,切记:tag的长度不能超过90字节.

3. 需要的预安装库
libxml2和log4cxx,这两个库都可以通过yum安装
```base
yum install log4cxx log4cxx-devel
yum install libxml2-devel
```
(因为源码安装需要改源码，建议用yum,附log4cxx的源码安装包,见utils/目录)
    源码安装步骤
http://blog.csdn.net/fhxpp_27/article/details/8280024

4. 编译运行
  make clean && make      //编译
  ./restart               //运行
  ./stop                  //关服

