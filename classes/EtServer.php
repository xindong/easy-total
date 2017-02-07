<?php

function debug($info)
{
    EtServer::$instance->debug($info);
}

function warn($info)
{
    EtServer::$instance->warn($info);
}

function info($info)
{
    EtServer::$instance->info($info);
}



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



class EtServer extends MyQEE\Server\Server
{
    /**
     * 计数器
     *
     * 受计数器限制只能存42亿,所以本系统会自动在1亿以上重置
     *
     * 总访问量应该是 `$this->counterX->get() * 100000000 + $this->counter->get();`
     * 或使用 `$this->getLogCount();` 获取
     *
     * @var Swoole\Atomic
     */
    public static $counter;

    /**
     * 计数器重置次数
     *
     * 每1亿重置1次, 所以总访问量应该是 `$this->counterX->get() * 100000000 + $this->counter->get()`;
     * 或使用 `$this->getLogCount();` 获取
     *
     * @var Swoole\Atomic
     */
    public static $counterX;

    /**
     * 启动时共享内存占用字节
     *
     * @var int
     */
    public static $startUseMemory;

    public static $configFile;

    public function __construct($configFile = 'server.yal')
    {
        $this->checkSystem();

        if (!function_exists('\\yaml_parse_file'))
        {
            self::warn('必须安装 yaml 插件');
            exit;
        }

        self::$configFile = $configFile;

        if (!is_file($configFile))
        {
            self::warn('指定的配置文件 "'.$configFile.'" 不存在');
            exit;
        }

        # 解析配置
        $conf = yaml_parse_file($configFile);

        if (!$conf)
        {
            self::warn("配置解析失败");
        }

        # 整合成系统需要的配置格式
        $config = [
            'server' => [
                'mode'                     => 'process',
                'shmop_mode'               => $conf['server']['shmop_mode'],
                'worker_memory_limit'      => $conf['server']['worker_memory_limit'],
                'task_worker_memory_limit' => $conf['server']['task_worker_memory_limit'],
                'unixsock_buffer_size'     => $conf['server']['unixsock_buffer_size'],
                'socket_block'             => $conf['server']['socket_block'],
            ],
            'hosts' => [
                'Main' => [
                    'type' => 'http',
                    'host' => $conf['manager']['host'],
                    'port' => $conf['manager']['port'],
                    'name' => $conf['manager']['name'],
                ],
                'EasyTotal' => [
                    'type' => 'tcp',
                    'host' => $conf['listen']['host'],
                    'port' => $conf['listen']['port'],
                    'conf' => [
                        'open_eof_check' => true,
                        'open_eof_split' => false,
                        'package_eof'    => "\n",
                    ],
                ],
            ],
            'log'      => $conf['log'],
            'swoole'   => $conf['swoole'],
            'clusters' => $conf['clusters'],
            'php'      => $conf['php'],
            'data'     => $conf['data'],
            'redis'    => $conf['redis'],
            'output'   => $conf['output'],
        ];

        # 是否使用共享内存模式
        define('SHMOP_MODE', $conf['server']['shmop_mode'] ? true : false);

        if (SHMOP_MODE && !function_exists('shmop_open'))
        {
            warn("你开启了共享内存模式, 但没有安装shmop扩展, see http://cn.php.net/manual/zh/book.shmop.php");
            exit;
        }

        # 传给底层配置
        parent::__construct($config);
    }

    /**
     * 在启动前执行
     */
    public function onBeforeStart()
    {
        # 初始化计数器
        self::$counter  = new \Swoole\Atomic();
        self::$counterX = new \Swoole\Atomic();

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
}