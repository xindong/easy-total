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
     * 数据保存的微妙时间
     *
     * @var swoole_atomic
     */
    public static $dataSaveTime;

    /**
     * 统计数据在保存失败后临时保存的文件路径
     *
     * @var string
     */
    public static $totalDumpFile;

    /**
     * 统计数据暂存内存表
     *
     * @var swoole_table
     */
    public static $totalTable;

    /**
     * 记录日志的时间
     *
     * @var swoole_table
     */
    public static $logTimeTable;

    /**
     * 记录任务状态的表
     *
     * @var swoole_table
     */
    public static $taskWorkerStatusTable;

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
     * HttpServer constructor.
     */
    public function __construct($configFile, $daemonize = false, $logPath = null)
    {
        if (!defined('SWOOLE_VERSION'))
        {
            warn("必须安装swoole插件,see http://www.swoole.com/");
            exit;
        }

        if (version_compare(SWOOLE_VERSION, '1.8', '<'))
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

        $lightBlue = "\x1b[36m";
        $end       = "\x1b[39m";
        echo "{$lightBlue}======= Swoole Config ========\n", json_encode($config['conf'], JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE), "{$end}\n";

        self::$config = $config;

        self::$totalDumpFile = (EtServer::$config['server']['dump_path'] ?: '/tmp/') . 'save_fail_total.txt';

        # 初始化计数器
        self::$counter  = new swoole_atomic();
        self::$counterX = new swoole_atomic();

        # 数据保存时间
        self::$dataSaveTime = new swoole_atomic();
        self::$dataSaveTime->set(time());

        # 创建共享内存表, 2 << 17 = 262144
        self::$totalTable = new swoole_table(2 << 17);
        self::$totalTable->column('value', swoole_table::TYPE_STRING, 1024);
        self::$totalTable->column('time', swoole_table::TYPE_INT, 10);
        self::$totalTable->create();

        # 记录日志的时间
        self::$logTimeTable = new swoole_table(1024);
        self::$logTimeTable->column('time', swoole_table::TYPE_INT, 10);
        self::$logTimeTable->column('update', swoole_table::TYPE_INT, 10);
        self::$logTimeTable->create();

        # 任务进程状态
        self::$taskWorkerStatusTable = new swoole_table($config['conf']['task_worker_num']);
        self::$taskWorkerStatusTable->column('status', swoole_table::TYPE_INT, 1);
        self::$taskWorkerStatusTable->column('time', swoole_table::TYPE_INT, 10);
        self::$taskWorkerStatusTable->create();

        # 加载历史数据
        self::loadData();
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
        self::$server = new swoole_websocket_server($host, $port);
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
        self::$server->on('Message',      [$this, 'onManagerMessage']);
        self::$server->on('Open',         [$this, 'onManagerOpen']);

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
            $this->taskWorker = new TaskWorker($server, $workerId - $server->setting['worker_num']);
            $this->taskWorker->init();

            info("Tasker Start, \$id = {$workerId}, \$pid = {$server->worker_pid}");
        }
        else
        {
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

        debug('onManagerStart');
    }

    public function onManagerStop($server)
    {
        debug('onManagerStop');
    }

    public static function saveTotalData($afterTime)
    {
        try
        {
            switch (self::$config['server']['data_type'])
            {
                case 'leveldb':
                    $rs = self::saveToLevelDB($afterTime);
                    break;

                case 'sqlite':
                    $rs = self::saveToSQLite($afterTime);
                    break;

                case 'redis':
                default:
                    $rs = self::saveToRedis($afterTime);
                    break;
            }
        }
        catch (Exception $e)
        {
            warn($e->getMessage());
            $rs = false;
        }

        return $rs;
    }

    public static function loadData()
    {
        # 从文件中读取上来
        if (is_file(self::$totalDumpFile))
        {
            $time = time();
            $data = @unserialize(file_get_contents(self::$totalDumpFile));
            if ($data)
            {
                foreach ($data as $k => $v)
                {
                    self::$totalTable->set($k, ['value' => $v, 'time' => $time]);
                }
            }
        }

        switch (self::$config['server']['data_type'])
        {
            case 'leveldb':
                self::loadFromLevelDB();
                break;

            case 'sqlite':
                self::loadFromSQLite();
                break;

            case 'redis':
            default:
                self::loadFromRedis();
                break;
        }

        debug("load total data count:" . count(self::$totalTable));
    }

    protected static function saveToRedis($afterTime)
    {
        $redis = self::getRedis();
        if (!$redis)
        {
            return false;
        }

        $rs = true;
        $i  = 0;

        foreach (self::$totalTable as $k => $v)
        {
            if ($v['time'] >= $afterTime)
            {
                if (false === $redis->hSet('total', $k, $v['value']))
                {
                    $rs = false;
                }
                else
                {
                    $i++;
                }
            }
        }

        info('成功保存了 ' .$i. ' 条统计信息数据');

        return $rs;
    }

    protected static function loadFromRedis()
    {
        $redis = self::getRedis();
        if (!$redis)
        {
            warn("redis ". self::$config['server']['data_link'] ." 连接失败, 请检查服务器");
            exit;
        }

        $i    = 0;
        $time = time();
        $it   = null;
        $redis->setOption(Redis::OPT_SCAN, Redis::SCAN_RETRY);
        while($arrKeys = $redis->hScan('total', $it))
        {
            foreach($arrKeys as $key => $value)
            {
                $i++;
                self::$totalTable->set($key, ['value' => $value, 'time' => $time]);
            }
        }

        info("loading data count: {$i}");
    }

    /**
     * 获取Redis连接
     *
     * @return bool|redis|RedisCluster
     */
    protected static function getRedis()
    {
        try
        {
            $link = explode(',', self::$config['server']['data_link']);
            if (count($link) > 1)
            {
                $redis = new RedisCluster(null, $link);
            }
            else
            {
                list($host, $port) = explode(':', $link[0]);
                $redis = new redis();

                if (false === $redis->connect($host, $port))
                {
                    throw new Exception('connect redis error');
                }
            }

            return $redis;
        }
        catch (Exception $e)
        {
            return false;
        }
    }

    protected static function saveToLevelDB($afterTime)
    {
        $options = [
            'create_if_missing' => true,    // if the specified database didn't exist will create a new one
            'error_if_exists'   => false,   // if the opened database exsits will throw exception
            'paranoid_checks'   => false,
            'block_cache_size'  => 2 << 10,
            'write_buffer_size' => 4 << 20,
            'block_size'        => 4096,
            'max_open_files'    => 1000,
            'block_restart_interval' => 16,
            'compression'       => LEVELDB_SNAPPY_COMPRESSION,
            'comparator'        => NULL,   // any callable parameter which returns 0, -1, 1
        ];

        /* default readoptions */
        $readoptions = [
            'verify_check_sum'  => false,
            'fill_cache'        => true,
            'snapshot'          => null
        ];

        /* default write options */
        $writeoptions = [
            'sync' => true
        ];

        $file = self::$config['server'] ['data_link'] . '/easy-total.leveldb.db';

        try
        {
            $db = new LevelDB($file, $options, $readoptions, $writeoptions);
            $rs = true;
            $i  = 0;

            foreach (self::$totalTable as $k => $v)
            {
                if ($v['time'] >= $afterTime)
                {
                    if (false === $db->set($k, $v['value']))
                    {
                        $rs = false;
                    }
                    else
                    {
                        $i++;
                    }
                }
            }

            info('成功保存了 ' . $i . '条统计信息数据');
        }
        catch (Exception $e)
        {
            warn($e->getMessage());
            $rs = false;
        }

        return $rs;
    }

    protected static function loadFromLevelDB()
    {
        if (!class_exists('LevelDB', false))
        {
            warn('你设置的数据类型是 LevelDB, 但没没有安装LevelDB扩展, 请先安装扩展, see https://github.com/reeze/php-leveldb');
            exit;
        }

        if (!self::$config['server'] ['data_link'])
        {
            warn('配置不正确, 必须有 sever[data_link] 参数');
            exit;
        }

        if (!is_writeable(self::$config['server'] ['data_link']))
        {
            warn("配置 sever[data_link] = " .self::$config['server'] ['data_link'].' , 目录不可写');
            exit;
        }

        $options = [
            'create_if_missing' => true,    // if the specified database didn't exist will create a new one
            'error_if_exists'   => false,   // if the opened database exsits will throw exception
            'paranoid_checks'   => false,
            'block_cache_size'  => 2 << 10,
            'write_buffer_size' => 4 << 20,
            'block_size'        => 4096,
            'max_open_files'    => 1000,
            'block_restart_interval' => 16,
            'compression'       => LEVELDB_SNAPPY_COMPRESSION,
            'comparator'        => NULL,   // any callable parameter which returns 0, -1, 1
        ];

        /* default readoptions */
        $readoptions = [
            'verify_check_sum'  => false,
            'fill_cache'        => true,
            'snapshot'          => null
        ];

        /* default write options */
        $writeoptions = [
            'sync' => true
        ];

        $file = self::$config['server'] ['data_link'] . '/easy-total.leveldb.db';
        $db   = new LevelDB($file, $options, $readoptions, $writeoptions);
        $it   = new LevelDBIterator($db);

        # 加载数据
        $time = time();
        $i    = 0;
        while($it->valid())
        {
            $i++;
            self::$totalTable->set($it->key(), ['value' => $it->current(), 'time' => $time]);
        }
        $db->close();
        info("loading data count: {$i}");

        unset($db);
    }


    protected static function saveToSQLite($afterTime)
    {
        try
        {
            $file = self::$config['server'] ['data_link'] . '/easy-total.sqlite.db';
            $db   = new SQLite3($file);
            $rs   = true;
            $i    = 0;

            foreach (self::$totalTable as $k => $v)
            {
                if ($v['time'] >= $afterTime)
                {
                    try
                    {
                        $sql = "INSERT OR REPLACE INTO total ('key', 'value', 'time') VALUES ('$k', '" . str_replace("'", "\\'", $v['value']) . "', " . $v['time'] . ")";
                        if (false === $db->query($sql))
                        {
                            $rs = false;
                        }
                        else
                        {
                            $i++;
                        }
                    }
                    catch (Exception $e)
                    {
                        $rs = false;
                        warn($e->getMessage());
                    }
                }
            }

            info('成功保存了 ' . $i . '条统计信息数据');
        }
        catch (Exception $e)
        {
            warn($e->getMessage());
            $rs = false;
        }

        return $rs;
    }

    protected static function loadFromSQLite()
    {
        if (!class_exists('SQLite3', false))
        {
            warn('你设置的数据类型是 SQLite, 但没没有安装 SQLite 扩展, 请先安装扩展');
            exit;
        }

        if (!self::$config['server'] ['data_link'])
        {
            warn('配置不正确, 必须有 sever[data_link] 参数');
            exit;
        }

        if (!is_writeable(self::$config['server'] ['data_link']))
        {
            warn("配置 sever[data_link] = " .self::$config['server'] ['data_link'].' , 目录不可写');
            exit;
        }

        $file = self::$config['server'] ['data_link'] . '/easy-total.sqlite.db';

        if (!$file)
        {
            $db = new SQLite3($file);
            $sql = 'CREATE TABLE total (key CHAR(255) PRIMARY KEY NOT NULL, value TEXT NOT NULL,time INT NOT NULL)';
            $db->query($sql);
            $db->close();
        }
        else
        {
            $db = new SQLite3($file);
            $i  = 0;
            $rs = $db->query('SELECT * FROM "total"');
            while ($row = $rs->fetchArray())
            {
                $i++;
                self::$totalTable->set($row['key'], ['value' => $row['value'], 'time' => $row['time']]);
            }
            info("loading data count: {$i}");

            $db->close();
        }
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

        # 强制设置成2
        $config['fluent']['dispatch_mode'] = 2;
    }
}