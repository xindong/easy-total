<?php

class FluentServer
{
    /**
     * @var array
     */
    public static $config = [];

    public static $configFile;

    /**
     * @var Manager
     */
    protected $manager;

    /**
     *
     * @var swoole_server
     */
    protected $server;

    /**
     * @var swoole_server_port
     */
    protected $serverPort;

    /**
     * Worker对象
     *
     * @var Worker
     */
    protected $worker;

    /**
     * TaskWorker对象
     *
     * @var TaskWorker
     */
    protected $taskWorker;

    /**
     * HttpServer constructor.
     */
    public function __construct($config_file, $daemonize = false, $logPath = null)
    {
        $red       = "\x1b[31m";
        $lightBlue = "\x1b[36m";
        $end       = "\x1b[39m";
        $error     = "{$red}✕{$end}";

        if (!defined('SWOOLE_VERSION'))
        {
            echo "{$error} {$red}必须安装swoole插件,see http://www.swoole.com/{$end}\n";
            exit;
        }

        if (version_compare(SWOOLE_VERSION, '1.8', '<'))
        {
            echo "{$error} {$red}swoole插件必须>=1.8版本{$end}\n";
            exit;
        }

        if (!$config_file)
        {
            echo "{$error} {$red}缺少参数-c配置{$end}\n";
            exit;
        }

        self::$configFile = realpath($config_file);
        if (!self::$configFile)
        {
            echo "{$error} {$red}配置文件{$config_file}不存在{$end}\n";
            exit;
        }

        # 读取配置
        $config = parse_ini_string(file_get_contents($config_file), true);

        if (!$config)
        {
            echo "{$error} {$red}config error.{$end}\n";
            exit;
        }

        # 设置参数
        if (isset($config['php']['error_reporting']))
        {
            error_reporting($config['php']['error_reporting']);
        }

        if (isset($config['php']['timezone']))
        {
            date_default_timezone_set($config['php']['timezone']);
        }

        if ($daemonize)
        {
            $config['conf']['daemonize'] = true;
        }

        if ($logPath)
        {
            $config['conf']['log_file'] = $logPath;
        }

        # 更新配置
        self::formatConfig($config);

        echo "{$lightBlue}======= Swoole Config ========\n", json_encode($config['conf'], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE), "{$end}\n";

        self::$config = $config;
    }

    /**
     * 启动服务
     */
    public function start()
    {
        # 开启管理子进程后有bug,先不启用
        $this->createServer();

        $this->serverPort = $this->server->listen(self::$config['server']['host'], self::$config['server']['port'], SWOOLE_SOCK_TCP);

        # 设置分包协议
        $config = [
            'open_eof_check' => true,
            'open_eof_split' => true,
            'package_eof'    => "\n",
        ];
        $this->serverPort->set($config);

        $this->serverPort->on('receive', function(swoole_server $server, $fd, $fromId, $data)
        {
            return $this->worker->onReceive($server, $fd, $fromId, $data);
        });

        $this->bind();

        if (self::$config['remote']['host'] && self::$config['remote']['port'])
        {
            # 载入远程 RemoteShell 控制指令功能
            require __DIR__ . '/RemoteShell.php';
            RemoteShell::listen($this->server, self::$config['remote']['host'], self::$config['remote']['port']);
        }

        $this->server->start();
    }

    /**
     * 创建服务
     *
     * @return swoole_process
     */
    protected function createServer()
    {
        $port = self::$config['manager']['port'] ?: 9200;
        $host = self::$config['manager']['host'] ?: '127.0.0.1';
        $this->server = new swoole_http_server($host, $port, SWOOLE_PROCESS);
        $this->server->set(self::$config['conf']);
    }


    /**
     * 绑定服务
     *
     * 注意, 主服务启的是 swoole_http_server 所有的接受数据的在 $this->serverPortManager 上另外绑定事件
     *
     * onConnect, onClose, onReceive 请绑定在 $this->serverPortManager 上
     *
     * 参考swoole的多端口监听
     *
     *  * http://wiki.swoole.com/wiki/page/525.html
     *  * http://wiki.swoole.com/wiki/page/528.html
     *
     * @return $this
     */
    protected function bind()
    {
        $this->server->on('WorkerStop',   [$this, 'onWorkerStop']);
        $this->server->on('Shutdown',     [$this, 'onShutdown']);
        $this->server->on('WorkerStart',  [$this, 'onWorkerStart']);
        $this->server->on('PipeMessage',  [$this, 'onPipeMessage']);
        $this->server->on('ManagerStart', [$this, 'onManagerStart']);
        $this->server->on('ManagerStop',  [$this, 'onManagerStop']);
        $this->server->on('Finish',       [$this, 'onFinish']);
        $this->server->on('Task',         [$this, 'onTask']);
        $this->server->on('Start',        [$this, 'onStart']);
        $this->server->on('Request',      [$this, 'onManagerRequest']);

        return $this;
    }

    public function onManagerRequest(swoole_http_request $request, swoole_http_response $response)
    {
        return $this->manager->onManagerRequest($request, $response);
    }

    public function onShutdown($server)
    {

    }

    public function onWorkerStop(swoole_server $server, $workerId)
    {
        if ($server->taskworker)
        {
            $type = 'Tasker';
        }
        else
        {
            $type = 'Worker';

            if ($this->worker)
            {
                # 保存数据
                $this->worker->dumpData();
            }
        }

        debug("{$type} Stop, \$id = {$workerId}, \$pid = {$server->worker_pid}");
    }

    /**
     * 进程启动
     *
     * @param swoole_server $server
     * @param $workerId
     */
    public function onWorkerStart(swoole_server $server, $workerId)
    {
        global $argv;

        # 实例化资源对象
        if ($server->taskworker)
        {
            self::setProcessName("php ". implode(' ', $argv) ." [task]");

            require (__DIR__ .'/TaskWorker.php');
            # 构造新对象
            $this->taskWorker = new TaskWorker($server, $workerId - $this->server->setting['worker_num']);
            $this->taskWorker->init();

            info("Tasker Start, \$id = {$workerId}, \$pid = {$server->worker_pid}");
        }
        else
        {
            self::setProcessName("php ". implode(' ', $argv) ." [worker]");

            require (__DIR__ .'/Manager.php');
            require (__DIR__ .'/Worker.php');

            debug("Worker Start, \$id = {$workerId}, \$pid = {$server->worker_pid}");

            $this->worker  = new Worker($server, $workerId);
            $this->worker->init();
            $this->manager = new Manager($server, $this->worker, $workerId);
        }
    }

    /**
     * 收到 sendMessage 来的消息
     *
     * @param swoole_server $server
     * @param $fromWorkerId
     * @param $message
     */
    public function onPipeMessage(swoole_server $server, $fromWorkerId, $message)
    {
        if ($this->server->taskworker)
        {
//            return $this->taskWorker->onPipeMessage($server, $fromWorkerId, $message);
        }
        else
        {
            return $this->worker->onPipeMessage($server, $fromWorkerId, $message);
        }
    }

    public function onFinish(swoole_server $server, $task_id, $data)
    {
        $this->worker->onFinish($server, $task_id, $data);
    }

    public function onTask(swoole_server $server, $taskId, $fromId, $data)
    {
        return $this->taskWorker->onTask($server, $taskId, $fromId, $data);
    }

    public function onStart(swoole_server $server)
    {
        info("Manager Server: http://".self::$config['manager']['host'].":".self::$config['manager']['port']."/");
        info("ServerStart, tcp://".self::$config['server']['host'].":".self::$config['server']['port']."/");
    }

    public function onManagerStart(swoole_server $server)
    {
        debug('onManagerStart');
    }

    public function onManagerStop($server)
    {
        debug('onManagerStop');
    }


    /**
     * 设置进程的名称
     *
     * @param $name
     */
    protected static function setProcessName($name)
    {
        if (function_exists('cli_set_process_title'))
        {
            @cli_set_process_title($name);
        }
        else
        {
            if (function_exists('swoole_set_process_name'))
            {
                @swoole_set_process_name($name);
            }
            else
            {
                trigger_error(__METHOD__ . ' failed. require cli_set_process_title or swoole_set_process_name.');
            }
        }
    }

    /**
     * 更新配置
     *
     * @param $config
     */
    protected static function formatConfig(& $config)
    {
        # 处理配置中一些不兼容的地方
        foreach ($config['conf'] as & $item)
        {
            if (is_string($item) && (strpos($item, '\\n') !== false || strpos($item, '\\r') !== false))
            {
                $item = str_replace(['\\n', '\\r'], ["\n", "\r"], $item);
            }
        }
        unset($item);

        foreach ($config['conf'] as & $item)
        {
            if (is_string($item) && (strpos($item, '\\n') !== false || strpos($item, '\\r') !== false))
            {
                $item = str_replace(['\\n', '\\r'], ["\n", "\r"], $item);
            }
        }

        if (!$config['conf']['worker_num'] > 0)
        {
            $config['conf']['worker_num'] = 8;
        }

        $config['conf']['max_request'] = (int)$config['conf']['max_request'];

        # 缓冲区文件大小
        if (!$config['fluent']['buffer_size'])
        {
            $config['fluent']['buffer_size'] = 2 * 1024 * 1024;
        }
        else
        {
            switch (strtolower(substr($config['fluent']['buffer_size'], -1)))
            {
                case 'm':
                    $config['fluent']['buffer_size'] = substr($config['fluent']['buffer_size'], 0, -1) * 1024 * 1024;
                    break;
                case 'k':
                    $config['fluent']['buffer_size'] = substr($config['fluent']['buffer_size'], 0, -1) * 1024;
                    break;
                case 'g':
                    $config['fluent']['buffer_size'] = substr($config['fluent']['buffer_size'], 0, -1) * 1024 * 1024 * 1024;
                    break;
                default:
                    $config['fluent']['buffer_size'] = (int)$config['fluent']['buffer_size'];
                    break;
            }
        }

        # 推动间隔时间
        if (!$config['fluent']['flush_interval'])
        {
            $config['fluent']['flush_interval'] = 1;
        }
        else
        {
            switch (strtolower(substr($config['fluent']['flush_interval'], -1)))
            {
                case 'm':
                    # 分
                    $config['fluent']['flush_interval'] = substr($config['fluent']['flush_interval'], 0, -1) * 60;
                    break;
                case 'h':
                    # 时
                    $config['fluent']['flush_interval'] = substr($config['fluent']['flush_interval'], 0, -1) * 3600;
                    break;
                case 'd':
                    # 天
                    $config['fluent']['flush_interval'] = substr($config['fluent']['flush_interval'], 0, -1) * 86400;
                    break;
                default:
                    $config['fluent']['buffer_size'] = (int)$config['fluent']['buffer_size'];
                    break;
            }
        }

        # 强制设置成2
        $config['fluent']['dispatch_mode'] = 2;
    }
}