<?php


class RemoteShell
{
    static $contexts = [];
    /**
     * @var swoole_server
     */
    static $serv;
    static $menu = [
        "e|exec [code]\t执行PHP代码",
        "w|worker [id]\t切换Worker进程",
        "l|list\t打印服务器所有连接的fd",
        "s|stats\t打印服务器状态",
        "i|info [fd]\t显示某个连接的信息",
        "h|help\t显示帮助界面",
        "q|quit\t退出终端",
    ];
    const PAGESIZE = 20;

    static function listen(swoole_server $serv, $host = "127.0.0.1", $port = 9599)
    {
        $port = $serv->listen($host, $port, SWOOLE_SOCK_TCP);
        if (!$port)
        {
            throw new Exception("listen fail.");
        }
        $port->set(array("open_eof_split" => true, 'package_eof' => "\r\n"));

        $port->on("Connect",     'RemoteShell::onConnect');
        $port->on("Close",       'RemoteShell::onClose');
        $port->on("Receive",     'RemoteShell::onReceive');
        $serv->on("PipeMessage", 'RemoteShell::onPipeMessage');
        self::$serv = $serv;
    }

    static function onConnect($serv, $fd, $reactor_id)
    {
        self::$contexts[$fd]['worker_id'] = $serv->worker_id;
        self::output($fd, implode("\r\n", self::$menu));
    }

    static function output($fd, $msg)
    {
        if (empty(self::$contexts[$fd]['worker_id']))
        {
            $msg .= "\r\n#" . self::$serv->worker_id . ">";
        }
        else
        {
            $msg .= "\r\n#" . self::$contexts[$fd]['worker_id'] . ">";
        }
        self::$serv->send($fd, $msg);
    }

    static function onClose($serv, $fd, $reactor_id)
    {
        unset(self::$contexts[$fd]);
    }

    static function onPipeMessage($serv, $from_worker_id, $message)
    {
        $arr = explode("\r\n", $message, 3);
        ob_start();
        eval($arr[2] . ";");
        self::output($arr[1], ob_get_clean());
    }

    static function onReceive(swoole_server $serv, $fd, $reactor_id, $data)
    {
        $args = explode(" ", $data, 2);
        $cmd  = trim($args[0]);
        unset($args[0]);
        switch ($cmd)
        {
            case 'w':
            case 'worker':
                if (empty($args[1]))
                {
                    self::output($fd, "invalid command.");
                    break;
                }
                self::$contexts[$fd]['worker_id'] = intval($args[1]);
                self::output($fd, "[switching to worker " . self::$contexts[$fd]['worker_id'] . "]");
                break;
            case 'e':
            case 'exec':
                //不在当前Worker进程
                if (self::$contexts[$fd]['worker_id'] != $serv->worker_id)
                {
                    $serv->sendMessage(__CLASS__ . "\r\n$fd\r\n" . $args[1], self::$contexts[$fd]['worker_id']);
                }
                else
                {
                    ob_start();
                    eval($args[1] . ";");
                    self::output($fd, ob_get_clean());
                }
                break;
            case 'h':
            case 'help':
                self::output($fd, implode("\r\n", self::$menu));
                break;
            case 's':
            case 'stats':
                $stats = $serv->stats();
                self::output($fd, var_export($stats, true));
                break;
            case 'i':
            case 'info':
                if (empty($args[1]))
                {
                    self::output($fd, "invalid command.");
                    break;
                }
                $fd   = intval($args[1]);
                $info = $serv->getClientInfo($fd);
                self::output($fd, var_export($info, true));
                break;
            case 'l':
            case 'list':
                $tmp = array();
                foreach ($serv->connections as $fd)
                {
                    $tmp[] = $fd;
                    if (count($tmp) > self::PAGESIZE)
                    {
                        self::output($fd, json_encode($tmp));
                        $tmp = array();
                    }
                }
                if (count($tmp) > 0)
                {
                    self::output($fd, json_encode($tmp));
                }
                break;
            case 'q':
            case 'quit':
                $serv->close($fd);
                break;
            default:
                self::output($fd, "unknow command[$cmd]");
                break;
        }
    }
}