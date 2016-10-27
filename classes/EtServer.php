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
     * 记录任务状态的表
     *
     * @var swoole_table
     */
    public static $taskWorkerStatus;

    public static $configFile;

    public function __construct($configFile = 'server.yaml')
    {
        $this->checkSystem();

        if (!function_exists('\\yaml_parse_file'))
        {
            self::warn('必须安装 yaml 插件');
            exit;
        }

        self::$configFile = $configFile;

        # 解析配置
        $conf = yaml_parse_file($configFile);

        if (!$conf)
        {
            self::warn("配置解析失败");
        }

        # 整合成系统需要的配置格式
        $config = [
            'server' => [
                'host' => $conf['manager']['host'],
                'port' => $conf['manager']['port'],
                'http' => [
                    'use'            => true,
                    'name'           => $conf['manager']['name'],
                    'manager'        => true,
                    'manager_prefix' => $conf['manager']['manager_prefix'],
                    'api'            => true,
                    'api_prefix'     => $conf['manager']['api_prefix'],
                    'websocket'      => false,
                ],
                'mode'                     => 'process',
                'sock_type'                => 1,
                'worker_memory_limit'      => $conf['server']['worker_memory_limit'],
                'task_worker_memory_limit' => $conf['server']['task_worker_memory_limit'],
                'unixsock_buffer_size'     => $conf['server']['unixsock_buffer_size'],
                'socket_block'             => $conf['server']['socket_block'],
                'log'                      => $conf['server']['log'],
            ],
            'sockets' => [
                'EasyTotal' => [
                    'link' => "tcp://{$conf['server']['host']}:{$conf['server']['port']}/",
                    'conf' => [
                        'open_eof_check' => true,
                        'open_eof_split' => false,
                        'package_eof'    => "\n",
                    ],
                ],
            ],
            'swoole'   => $conf['swoole'],
            'clusters' => $conf['clusters'],
            'php'      => $conf['php'],
            'data'     => $conf['data'],
            'redis'    => $conf['redis'],
            'output'   => $conf['output'],
        ];

        # 传给底层配置
        parent::__construct($config);
    }

    /**
     * 在启动前执行
     */
    public function onBeforeStart()
    {
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
        self::$taskWorkerStatus = new \Swoole\Table(bindec(str_pad(1, strlen(decbin(self::$config['conf']['task_worker_num'])), 0)) * 16);
        self::$taskWorkerStatus->column('status', swoole_table::TYPE_INT, 1);
        self::$taskWorkerStatus->column('time',   swoole_table::TYPE_INT, 10);
        self::$taskWorkerStatus->column('pid',    swoole_table::TYPE_INT, 10);
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