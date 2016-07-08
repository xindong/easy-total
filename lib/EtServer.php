<?php


/**
 * 获取按时间分组的key
 *
 * @param $time
 * @param $limit
 * @param $type
 * @return int
 */
function getTimeKey($time, $limit, $type)
{
    # 放在缓存里
    static $cache = [];

    $key = "{$time}{$limit}{$type}";
    if (isset($cache[$key]))return $cache[$key];

    # 按时间处理分组
    switch ($type)
    {
        case 'm':
            # 月   201600
            $timeKey   = 100 * date('Y', $time);
            $timeLimit = date('m', $time);
            break;

        case 'w':
            # 当年中第N周  201600
            $timeKey   = 100 * date('Y', $time);
            $timeLimit = date('W', $time);

            if ($timeLimit > 50 && date('m', $time) == 1)
            {
                # 如果是1月，却出现 52, 53 这样的数值，表示是上年的第 52, 53 周，这边调整成第 0 周
                $timeLimit = 0;
            }
            break;

        case 'd':
            # 天   2016000
            $timeKey   = 1000 * date('Y', $time);
            # 当年中的第N天, 0-365
            $timeLimit = date('z', $time) + 1;
            break;

        case 'M':
        case 'i':
            # 分钟  201604100900
            $timeKey   = 100 * date('YmdH', $time);
            $timeLimit = date('i', $time);
            break;

        case 's':
            # 秒   20160410090900
            $timeKey   = 100 * date('YmdHi', $time);
            $timeLimit = date('s', $time);
            break;

        case 'h':
        default:
            # 小时        2016041000
            $timeKey   = 100 * date('Ymd', $time);
            $timeLimit = date('H', $time);
            break;
    }

    if ($limit > 1)
    {
        # 按 $job['groupTime']['limit'] 中的数值分组
        if ($type == 'm' || $type == 'd')
        {
            # 除月份、天是从1开始，其它都是从0开始
            $timeKey += 1 + $limit * floor($timeLimit/ $limit);
        }
        else
        {
            $timeKey += $limit * floor($timeLimit/ $limit);
        }
    }
    else
    {
        $timeKey += $timeLimit;
    }

    if (count($cache) > 200)
    {
        # 清理下
        $cache = array_slice($cache, -10, null, true);
    }

    $cache[$key] = $timeKey;

    return $timeKey;
}


/**
 * 获取下一个时间分组的时间戳
 *
 * @param $timeKey
 * @param $limit
 * @param $type
 * @return int|mixed
 */
function getNextTimestampByTimeKey($timeKey, $limit, $type)
{
    static $cache = [];
    $key = "$timeKey$limit$type";

    if (isset($cache[$key]))return $cache[$key];
    $year     = intval(substr($timeKey, 0, 4));
    $nextYear = strtotime(($year + 1) .'-01-01 00:00:00');
    switch ($type)
    {
        case 'm':
            # 月
            preg_match('#(\d{4})(\d{2})#', $timeKey, $m);
            $month = $limit + $m[2];
            if ($month > 12)
            {
                $m[1] += 1;
                $month = 1;
            }

            $time = strtotime("{$year}-{$month}-01 00:00:00");
            $time = min($nextYear, $time);
            break;

        case 'w':
            # 周
            preg_match('#(\d{4})(\d{2})#', $timeKey, $m);
            $time = strtotime("{$year}-01-01 00:00:00") + ($m[2] + $limit) * (86400 * 7);
            $time = min($nextYear, $time);
            break;

        case 'd':
            preg_match('#(\d{4})(\d{3})#', $timeKey, $m);
            var_dump($limit);
            $time = strtotime("{$year}-01-01 00:00:00") + ($m[2] - 1 + $limit) * 86400;
            $time = min($nextYear, $time);
            break;

        case 'M':
        case 'i':
            # 分钟 201604100900
            preg_match('#(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})#', $timeKey, $m);
            $time  = strtotime("{$year}-{$m[2]}-{$m[3]} {$m[4]}:00:00");
            $time += min(3600, 60 * ($m[5] + $limit));
            break;

        case 's':
            # 秒 20160410090000
            preg_match('#(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})#', $timeKey, $m);
            $time  = strtotime("{$year}-{$m[2]}-{$m[3]} {$m[4]}:{$m[5]}:00");
            $time += min(60, ($m[6] + $limit));
            break;

        case 'h':
        default:
            # 小时 2016041000
            preg_match('#(\d{4})(\d{2})(\d{2})(\d{2})#', $timeKey, $m);
            $time  = strtotime("{$year}-{$m[2]}-{$m[3]} 00:00:00");
            $time += min(86400, 3600 * ($m[4] + $limit));

            break;
    }

    if (count($cache) > 100)
    {
        $cache = array_slice($cache, -10, null, true);
    }

    $cache[$key] = $time;
    return $time;
}


class EtServer
{
    /**
     * @var array
     */
    public static $config = [];

    public static $configFile;

    /**
     *
     * @var swoole_server
     */
    public static $server;

    /**
     * @var Manager
     */
    protected $manager;

    /**
     * @var swoole_server_port
     */
    protected $serverPort;

    /**
     * Worker对象
     *
     * @var MainWorker
     */
    protected $worker;

    /**
     * TaskWorker对象
     *
     * @var TaskWorker
     */
    protected $taskWorker;

    /**
     * 记录任务状态的表
     *
     * @var swoole_table
     */
    public static $taskWorkerStatus;

    /**
     * 记录数据统计的对象
     *
     * @var array
     */
    public static $jobsTable = [];

    /**
     * 计数器
     *
     * 受计数器限制只能存42亿,所以本系统会自动在1亿以上重置
     *
     * 总访问量应该是 `$this->counterX->get() * 100000000 + $this->counter->get();`
     * 或使用 `$this->getLogCount();` 获取
     *
     * @var swoole_atomic
     */
    public static $counter;

    /**
     * 计数器重置次数
     *
     * 每1亿重置1次, 所以总访问量应该是 `$this->counterX->get() * 100000000 + $this->counter->get()`;
     * 或使用 `$this->getLogCount();` 获取
     *
     * @var swoole_atomic
     */
    public static $counterX;

    /**
     * 启动时共享内存占用字节
     *
     * @var int
     */
    public static $startUseMemory;

    /**
     * HttpServer constructor.
     */
    public function __construct($configFile, $daemonize = false, $logPath = null)
    {
        if (!defined('SWOOLE_VERSION'))
        {
            warn("必须安装swoole插件, see http://www.swoole.com/");
            exit;
        }

        if (version_compare(SWOOLE_VERSION, '1.8.0', '<'))
        {
            warn("swoole插件必须>=1.8版本");
            exit;
        }



        if (!$configFile)
        {
            $configFile = '/etc/easy-total.ini';
        }

        self::$configFile = realpath($configFile);
        if (!self::$configFile)
        {
            warn("配置文件{$configFile}不存在");
            exit;
        }

        # 读取配置
        $config = parse_ini_string(file_get_contents($configFile), true);

        if (!$config)
        {
            warn("config error");
            exit;
        }

        # 是否使用共享内存模式
        define('SHMOP_MODE', $config['server']['shmop_mode'] ? true : false);

        if (SHMOP_MODE && !function_exists('shmop_open'))
        {
            warn("你开启了共享内存模式, 但没有安装shmop扩展, see http://cn.php.net/manual/zh/book.shmop.php");
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

        if (isset($config['server']['unixsock_buffer_size']) && $config['server']['unixsock_buffer_size'] > 1000)
        {
            # 修改进程间通信的UnixSocket缓存区尺寸
            ini_set('swoole.unixsock_buffer_size', $config['server']['unixsock_buffer_size']);
        }

        if ($logPath)
        {
            $config['conf']['log_file'] = $logPath;
        }

        if ($daemonize)
        {
            $config['conf']['daemonize'] = true;
        }

        # 更新配置
        self::formatConfig($config);


        $lightBlue = "\x1b[36m";
        $end       = "\x1b[39m";
        info("{$lightBlue}======= Swoole Config ========\n". json_encode($config['conf'], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE). "{$end}");

        self::$config   = $config;
        # 初始化计数器
        self::$counter  = new swoole_atomic();
        self::$counterX = new swoole_atomic();

        # 当前进程的pid
        $pid     = getmypid();
        $memory1 = 0;

        foreach (explode("\n", trim(`ps -eorss,pid | grep $pid`)) as $item)
        {
            if (preg_match('#(\d+)[ ]+(\d+)#', trim($item), $m))
            {
                if ($m[2] == $pid)
                {
                    $memory1 = $m[1];
                }
            }
        }

        # 任务进程状态, 必须是2的指数
        self::$taskWorkerStatus = new swoole_table(bindec(str_pad(1, strlen(decbin($config['conf']['task_worker_num'])), 0)) * 16);
        self::$taskWorkerStatus->column('status', swoole_table::TYPE_INT, 1);
        self::$taskWorkerStatus->column('time', swoole_table::TYPE_INT, 10);
        self::$taskWorkerStatus->column('pid', swoole_table::TYPE_INT, 10);
        self::$taskWorkerStatus->create();

        # 列出当前任务的内存
        $memory2 = $memory1;
        foreach (explode("\n", trim(`ps -eorss,pid | grep $pid`)) as $item)
        {
            if (preg_match('#(\d+)[ ]+(\d+)#', trim($item), $m))
            {
                if ($m[2] == $pid)
                {
                    $memory2 = $m[1];
                }
            }
        }
        self::$startUseMemory = ($memory2 - $memory1) * 1024;

        debug("pid is: $pid");
        info("memory block data use memory: " . number_format(($memory2 - $memory1) / 1024, 3) . 'MB');
    }

    /**
     * 启动服务
     */
    public function start()
    {
        # 开启管理子进程后有bug,先不启用
        $this->createServer();

        $this->serverPort = self::$server->listen(self::$config['server']['host'], self::$config['server']['port'], SWOOLE_SOCK_TCP);

        # 设置分包协议
        $config = [
            'open_eof_check' => true,
            'open_eof_split' => false,
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
            RemoteShell::listen(self::$server, self::$config['remote']['host'], self::$config['remote']['port']);
        }

        # 设置不阻塞
        swoole_async_set(['socket_dontwait' => 1]);

        self::$server->start();
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
        #self::$server = new swoole_websocket_server($host, $port);
        self::$server = new swoole_http_server($host, $port);
        self::$server->set(self::$config['conf']);
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
        self::$server->on('WorkerStop',   [$this, 'onWorkerStop']);
        self::$server->on('Shutdown',     [$this, 'onShutdown']);
        self::$server->on('WorkerStart',  [$this, 'onWorkerStart']);
        self::$server->on('PipeMessage',  [$this, 'onPipeMessage']);
        self::$server->on('ManagerStart', [$this, 'onManagerStart']);
        self::$server->on('ManagerStop',  [$this, 'onManagerStop']);
        self::$server->on('Finish',       [$this, 'onFinish']);
        self::$server->on('Task',         [$this, 'onTask']);
        self::$server->on('Start',        [$this, 'onStart']);
        self::$server->on('Request',      [$this, 'onManagerRequest']);


        #self::$server->on('Message',      [$this, 'onManagerMessage']);
        #self::$server->on('Open',         [$this, 'onManagerOpen']);

        return $this;
    }

    /**
     * 管理端的HTTP协议接口
     *
     * @param swoole_http_request $request
     * @param swoole_http_response $response
     * @return mixed
     */
    public function onManagerRequest(swoole_http_request $request, swoole_http_response $response)
    {
        return $this->manager->onRequest($request, $response);
    }

    /**
     * 管理端的webSocket协议收到消息
     *
     * @param swoole_server $server
     * @param swoole_websocket_frame $frame
     * @return mixed
     */
    public function onManagerMessage(swoole_websocket_server $server, swoole_websocket_frame $frame)
    {
        return $this->manager->onMessage($server, $frame);
    }

    /**
     * 管理webSocket端打开连接
     *
     * @param swoole_websocket_server $server
     * @param swoole_http_request $request
     * @return mixed
     */
    public function onManagerOpen(swoole_websocket_server $server, swoole_http_request $request)
    {
        return $this->manager->onOpen($server, $request);
    }

    public function onShutdown($server)
    {
        debug('server stopped');
    }

    public function onWorkerStop(swoole_server $server, $workerId)
    {
        if ($server->taskworker)
        {
            $type = 'Tasker';

            if ($this->taskWorker)
            {
                $this->taskWorker->shutdown();
            }
        }
        else
        {
            $type = 'Worker';

            if ($this->worker)
            {
                # 保存数据
                $this->worker->shutdown();

                if ($workerId === 0)
                {
                    for($i = 1; $i < $server->setting['task_worker_num']; $i++)
                    {
                        $server->task('exit');
                    }
                }
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
            # 任务序号
            $taskId = $workerId - $server->setting['worker_num'];

            # 内存限制
            ini_set('memory_limit', self::$config['server']['task_worker_memory_limit'] ?: '6G');
            if ($taskId == 0)
            {
                info("current server task worker memory limit is: ". ini_get('memory_limit'));
            }

            self::setProcessName("php ". implode(' ', $argv) ." [task]");

            require (__DIR__ .'/TaskWorker.php');

            # 构造新对象
            $this->taskWorker = new TaskWorker($server, $taskId, $workerId);
            $this->taskWorker->init();

            info("Tasker Start, \$id = {$workerId}, \$pid = {$server->worker_pid}");
        }
        else
        {
            # 内存限制
            ini_set('memory_limit', self::$config['server']['worker_memory_limit'] ?: '2G');
            if ($workerId == 0)
            {
                info("current server worker memory limit is: ". ini_get('memory_limit'));
            }

            self::setProcessName("php ". implode(' ', $argv) ." [worker]");

            require (__DIR__ .'/Manager.php');
            require (__DIR__ .'/MainWorker.php');

            debug("Worker Start, \$id = {$workerId}, \$pid = {$server->worker_pid}");

            $this->worker  = new MainWorker($server, $workerId);
            $this->worker->init();
            $this->manager = new Manager($server, $this->worker, $workerId);


            if ($workerId == 0)
            {
                # 计数器只支持42亿的计数, 所以每小时检查计数器是否快溢出
                swoole_timer_tick(1000 * 60 * 60, function()
                {
                    if (($count = self::$counter->get()) > 100000000)
                    {
                        # 将1亿的余数记录下来
                        self::$counter->set($count % 100000000);
                        self::$counterX->add(intval($count / 100000000));
                    }
                });
            }
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
        if ($server->taskworker)
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
        info("Manager Server: http://".self::$config['manager']['host'].":".self::$config['manager']['port']. '/admin/');
        info("ServerStart, tcp://".self::$config['server']['host'].":".self::$config['server']['port']."/");
    }

    public function onManagerStart(swoole_server $server)
    {
        global $argv;
        self::setProcessName("php ". implode(' ', $argv) ." [manager]");

        debug('manager start');
    }

    public function onManagerStop($server)
    {
        debug('manager stopped');
    }

    /**
     * 获取已请求的所有log数量
     *
     * @return int
     */
    public static function getCount()
    {
        return self::$counterX->get() * 100000000 + self::$counter->get();
    }

    /**
     * 设置进程的名称
     *
     * @param $name
     */
    public static function setProcessName($name)
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

        # 临时存档目录
        if (!EtServer::$config['server']['dump_path'])
        {
            EtServer::$config['server']['dump_path'] = '/tmp/';
        }

        # 强制设置成2
        $config['fluent']['dispatch_mode'] = 2;

        if (!$config['conf']['task_tmpdir'])
        {
            $config['conf']['task_tmpdir'] = is_dir('/dev/shm') ? '/dev/shm' : '/tmp/';
        }
        else
        {
            if (!is_dir($config['conf']['task_tmpdir']))
            {
                debug("change task_tmpdir from {$config['conf']['task_tmpdir']} to /tmp/");
                $config['conf']['task_tmpdir'] = '/tmp/';
            }
        }
    }
}