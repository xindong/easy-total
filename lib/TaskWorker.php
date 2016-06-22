<?php

require_once __DIR__ .'/DataDriver.php';
require_once __DIR__ .'/TaskProcess.php';

class TaskWorker
{
    /**
     * 任务进程ID, 从0开始
     *
     * @var int
     */
    protected $taskId;

    /**
     * 进程ID, 序号接着work进程序号后
     *
     * @var
     */
    protected $workerId;

    /**
     * @var swoole_server
     */
    protected $server;

    /**
     * 当前进程启动时间
     *
     * @var int
     */
    protected $startTime;

    /**
     * 记录各个功能的执行时间
     *
     * @var array
     */
    protected $doTime = [];

    /**
     * 任务数据处理进程对象
     *
     * @var taskProcess
     */
    protected $taskProcess;

    /**
     * 任务共享数据对象
     *
     * @var swoole_table
     */
    protected $jobsTable;

    protected $autoPause = false;

    /**
     * 已经到排队时间但是队列繁忙延期的任务数
     *
     * @var int
     */
    protected $delayJobCount = 0;

    /**
     * @var array
     */
    public static $jobs = [];

    public static $serverName;

    /**
     * 当程序需要终止时如果无法把数据推送出去时临时导出的文件路径（确保数据安全）
     *
     * @var string
     */
    public static $dumpFile;

    /**
     * 数据块大小, 默认 1024
     *
     * @var int
     */
    protected static $dataBlockSize;

    /**
     * 数据块数量, 默认 2 << 16 即 131072
     *
     * @var int
     */
    protected static $dataBlockCount;

    public function __construct(swoole_server $server, $taskId, $workerId)
    {
        $this->server    = $server;
        $this->taskId    = $taskId;
        $this->workerId  = $workerId;
        $this->startTime = time();

        $hash = substr(md5(EtServer::$configFile), 16, 8);
        # 设置配置
        self::$dataBlockCount      = EtServer::$config['server']['data_block_count'];
        self::$dataBlockSize       = EtServer::$config['server']['data_block_size'];
        self::$serverName          = EtServer::$config['server']['host'] .':'. EtServer::$config['server']['port'];
        self::$dumpFile            = EtServer::$config['server']['dump_path'] .'easy-total-task-dump-'. $hash . '-'. $taskId .'.txt';
        TaskProcess::$dumpFile     = EtServer::$config['server']['dump_path'] .'easy-total-task-process-dump-'. $hash. '-'. $taskId .'.txt';
        TaskProcess::$dataConfig   = EtServer::$config['data'];
        TaskProcess::$redisConfig  = EtServer::$config['redis'];
        TaskProcess::$outputConfig = EtServer::$config['output'];

        if ($taskId > 0)
        {
            # 创建子进程
            # $taskId = 0 的进程用于清理数据, 不分配任务
            $task = new TaskProcess($this->taskId);
            $task->start();
            $this->taskProcess = $task;
            $this->jobsTable   = EtServer::$jobsTable[$taskId];
        }
    }

    public function init()
    {
        $this->loadDumpData();
    }

    /**
     * @param swoole_server $server
     * @param $taskId
     * @param $fromId
     * @param $data
     * @return mixed
     */
    public function onTask(swoole_server $server, $taskId, $fromId, $data)
    {
        try
        {
            if (is_object($data))
            {
                # 获取数据类型
                $type = get_class($data);
            }
            else
            {
                $data = explode('|', $data);
                $type = $data[0];
            }

            switch ($type)
            {
                case 'DataJob':
                    # 任务数据
                    /**
                     * @var DataJob $data;
                     */
                    $uniqueId = $data->uniqueId;
                    if (isset(self::$jobs[$uniqueId]))
                    {
                        # 合并任务
                        self::$jobs[$uniqueId]->merge($data);
                    }
                    else
                    {
                        # 设置一个任务投递时间
                        $data->taskTime        = self::getJobTime($data);
                        self::$jobs[$uniqueId] = $data;
                    }

                    # 当前任务数
                    if (!$this->autoPause && false === $this->checkStatus())
                    {
                        # 积压的任务数非常多
                        info("Task#$this->taskId jobs is too much, now notify server pause accept new data.");
                        $this->notifyWorkerPause();
                    }
                    break;

                case 'job':
                    # 每3秒会被调用1次
                    $this->taskJob();
                    break;

                case 'clean':
                    # 清理数据, 只有 taskId = 0 的进程会被调用
                    $this->clean();
                    break;
            }
        }
        catch (Exception $e)
        {
            warn($e->getMessage());
        }

        # 更新内存占用
        if (!isset($this->doTime['updateMemory']) || time() - $this->doTime['updateMemory'] >= 60)
        {
            list($redis) = self::getRedis();
            if ($redis)
            {
                /**
                 * @var Redis $redis
                 */
                $redis->hSet('server.memory', self::$serverName .'_'. $this->workerId, serialize([memory_get_usage(true), time(), self::$serverName, $this->workerId]));
            }
            $this->doTime['updateMemory'] = time();
        }

        # 标记状态为成功
        $this->updateStatus(true);

        # 任务数小余一定程度后继续执行
        if ($this->autoPause)
        {
            if (count($this->jobsTable) < 1000)
            {
                info("Task#$this->taskId now notify server continue accept new data.");
                $this->notifyWorkerContinue();
            }
        }
//
//        # 如果启动超过1小时
//        if (time() - $this->startTime > 3600)
//        {
//            if (mt_rand(1, 200) === 1)
//            {
//                # 重启进程避免数据溢出、未清理数据占用超大内存
//                $this->shutdown();
//
//                info('now restart task worker: '. $this->taskId);
//
//                exit(0);
//            }
//        }
    }

    /**
     * 当积压的数据很多时, 通知进程暂停接受新数据
     */
    protected function notifyWorkerPause()
    {
        $this->autoPause = true;

        for ($i = 0; $i < $this->server->setting['worker_num']; $i++)
        {
            $this->server->sendMessage('pause', $i);
        }
    }

    /**
     * 通知进程继续处理
     */
    protected function notifyWorkerContinue()
    {
        $this->autoPause = false;

        for ($i = 0; $i < $this->server->setting['worker_num']; $i++)
        {
            $this->server->sendMessage('continue', $i);
        }
    }

    /**
     * 处理数据任务
     */
    protected function taskJob()
    {
        $now = time();
        $num = 0;
        $max = max(0, intval(self::$dataBlockCount * 0.8) - count($this->jobsTable));

        # 更新延期任务计数
        $this->delayJobCount = 0;
        foreach (self::$jobs as $uniqueId => $job)
        {
            /**
             * @var DataJob $job
             */
            if ($now >= $job->taskTime)
            {
                if ($num >= $max)
                {
                    # 超过每次投递的上线额
                    $this->delayJobCount++;
                }
                elseif ($this->pushJob($job))
                {
                    # 添加任务数据成功
                    unset(self::$jobs[$uniqueId]);
                    $num++;
                }
                else
                {
                    $this->delayJobCount++;
                }
            }
        }

        $this->updateStatus();
    }

    /**
     * 投递任务
     *
     * @param DataJob $job
     * @return bool
     */
    protected function pushJob(DataJob $job)
    {
        $data           = [];
        $data['value']  = serialize($job);
        $data['index']  = 0;
        $data['time']   = time();
        $data['length'] = ceil(strlen($data['value']) / self::$dataBlockSize);
        $jobTable       = $this->jobsTable;
        $key            = md5($job->uniqueId . microtime(1));

        if ($data['length'] > 1)
        {
            # 超过1000字符则分段截取
            # 从后面设置是避免设置的第一个数据后还没有设置完成就被子进程读取
            for($i = $data['length'] - 1; $i >= 0; $i--)
            {
                $tmp = [
                    'index'  => $i,
                    'length' => $data['length'],
                    'time'   => $data['time'],
                    'value'  => substr($data['value'], $i * self::$dataBlockSize, self::$dataBlockSize),
                ];

                $tmpKey = $i > 0 ? "{$key}_{$i}" : $key;
                if (!$jobTable->set($tmpKey, $tmp))
                {
                    # 插入失败
                    warn("Task#$this->taskId set swoole_table fail, key: $tmpKey");
                    return false;
                }
            }

            return true;
        }
        else
        {
            return $jobTable->set($key, $data);
        }
    }

    /**
     * 检查状态
     *
     * true 表示可以, false 表示繁忙
     *
     * @param bool $checkProcess
     * @return bool
     */
    protected function checkStatus()
    {
        $dataCount = count($this->jobsTable);
        if ($dataCount + $this->delayJobCount > self::$dataBlockCount * 0.8)
        {
            # 积累的任务数已经很多了
            warn("Task#$this->taskId queue data is to much. now count: {$dataCount}, delay job count: {$this->delayJobCount}, max queue is ". self::$dataBlockCount);

            return false;
        }

        return true;
    }


    public function shutdown()
    {
        # 将数据保存下来
        $this->dumpData();

        # 清空数据
        self::$jobs = [];

        $this->taskProcess->close();
    }


    /**
     * 在程序退出时保存数据
     */
    public function dumpData()
    {
        if (self::$jobs)
        {
            # 写入到临时数据里, 下次启动时载入
            foreach (self::$jobs as $job)
            {
                file_put_contents(self::$dumpFile, serialize($job) ."\r\n", FILE_APPEND);
            }
        }
    }

    /**
     * 加载dump出去的数据
     */
    protected function loadDumpData()
    {
        if (is_file(self::$dumpFile))
        {
            foreach (explode("\r\n", file_get_contents(self::$dumpFile)) as $item)
            {
                $tmp = @unserialize($item);
                if ($tmp && is_object($tmp))
                {
                    /**
                     * @var DataJob $tmp
                     */
                    if (isset(self::$jobs[$tmp->uniqueId]))
                    {
                        self::$jobs[$tmp->uniqueId]->merge($tmp);
                    }
                    else
                    {
                        self::$jobs[$tmp->uniqueId] = $tmp;
                    }
                }
            }

            info("Task#$this->taskId reload ". count(self::$jobs) . ' jobs from dump file.');

            unlink(self::$dumpFile);
        }
    }

    /**
     * 清理redis中的数据
     *
     * @param $key
     */
    public function clearSeriesDataByKey($key)
    {
        /**
         * @var Redis $redis
         * @var SimpleSSDB $ssdb
         */
        list($redis, $ssdb) = self::getRedis();
        if (false === $redis)return false;

        # 监控统计数据的key
        $keys = [];
        $time = time();
        for ($i = 0; $i <= 20; $i++)
        {
            $k1     = date('Ymd', $time);
            $keys[] = "counter.pushtime.$k1.$key";
            $keys[] = "counter.total.$k1.$key";
            $keys[] = "counter.time.$k1.$key";
            $time  -= 86400;
        }

        if ($ssdb)
        {
            foreach ($keys as $k)
            {
                $ssdb->hclear($k);
            }

            foreach (['total', 'dist', 'join'] as $item)
            {
                while ($keys = $ssdb->hlist("{$item},{$key},", "{$item},{$key},z", 100))
                {
                    foreach ($keys as $k)
                    {
                        $ssdb->hclear($k);
                    }
                }
            }

            $ssdb->hclear("series.app.$key");
        }
        else
        {
            # 移除监控统计数据
            $redis->del($keys);

            # 移除统计数据
            if ($keys = $redis->sMembers("totalKeys,$key"))
            {
                $redis->del($keys);
            }

            # 移除唯一序列数据
            if ($keys = $redis->sMembers("distKeys,$key"))
            {
                $redis->del($keys);
            }

            # 移除序列信息
            $redis->del("series.app.$key");
        }

        $redis->close();
        if ($ssdb)$ssdb->close();

        return true;
    }

    /**
     * 每天清理数据
     *
     * @return bool
     */
    protected function clean()
    {
        static $lastClean = null;
        if (null === $lastClean)
        {
            $lastClean = time();
        }

        if (time() - $lastClean > 3600)
        {
            list($redis, $ssdb) = self::getRedis();
            $this->cleanOldData($redis, $ssdb);
            $lastClean = time();
        }
    }

    /**
     * 清理一些旧数据, 1小时执行1次
     *
     * @param $redis
     * @param $ssdb
     * @return bool
     */
    protected function cleanOldData($redis, $ssdb)
    {
        /**
         * @var Redis $redis
         * @var SimpleSSDB $ssdb
         */
        if (false === $redis)return false;

        $time = time();

        # 清理已经删除的任务
        $queries = array_map('unserialize', $redis->hGetAll('queries'));
        foreach ($queries as $key => $query)
        {
            if ($query['deleteTime'] > 0 && $time - $query['deleteTime'] > 86400)
            {
                unset($queries[$key]);

                $redis->hDel('queries', $key);
                info("clean data, remove sql({$key}): {$query['sql']}");
            }
        }

        $updateSeries = [];
        $series       = array_map('unserialize', $redis->hGetAll('series'));
        foreach ($series as $key => $item)
        {
            if ($item['queries'])
            {
                # 遍历所有的查询关系设置
                /*
                 exp:
                 $item['queries'] = [
                    '1d' => ['abcdef123123', 'abc12312353'],
                    '1h' => ['abcdef123123', 'abc12312353'],
                ];
                 */
                foreach ($item['queries'] as $st => $keys)
                {
                    foreach ($keys as $k => $v)
                    {
                        if (!isset($queries[$v]))
                        {
                            unset($item[$st][$k]);
                            if (!$item[$st][$k])
                            {
                                unset($item[$st][$k]);
                            }
                            else
                            {
                                # 更新
                                $item[$st][$k] = array_values($item[$st][$k]);
                            }

                            # 放到需要处理的变量里
                            $updateSeries[$key] = $item;
                        }
                    }
                }
            }
            else
            {
                $updateSeries[$key] = $item;
            }
        }

        # 进行清理数据操作
        if ($updateSeries)
        {
            foreach ($updateSeries as $key => $item)
            {
                if (!$item['queries'])
                {
                    # 没有关联的查询了, 直接删除这个序列
                    if ($this->clearSeriesDataByKey($key))
                    {
                        # 移除任务
                        $redis->hDel('series', $key);
                    }

                    # 更新状态
                    $this->updateStatus(false);
                }
            }
        }


        # 清理每天的统计数据, 只保留10天内的
        $time = time();
        $k1   = date('Y-m-d', $time - 86400 * 12);
        $k2   = date('Y-m-d', $time - 86400 * 11);

        if ($ssdb)
        {
            foreach (['total', 'time', 'pushtime'] as $item)
            {
                foreach (['counter', 'counterApp'] as $k0)
                {
                    while ($keys = $ssdb->hlist("$k0.$item.$k1", "counter.$item.$k2", 100))
                    {
                        # 列出key
                        foreach ($keys as $k)
                        {
                            # 清除
                            $ssdb->hclear($k);
                        }
                    }
                }
            }

            $ssdb->hclear("counter.allpushtime.$k1");
            $ssdb->hclear("counter.allpushtime.$k2");
        }
        else
        {
            foreach (['total', 'time', 'pushtime'] as $item)
            {
                foreach (['counter', 'counterApp'] as $k0)
                {
                    $keys = $redis->keys("$k0.$item.$k1.*");
                    if ($keys)
                    {
                        foreach ($keys as $k)
                        {
                            $redis->del($k);
                        }
                    }

                    $keys = $redis->keys("$k0.$item.$k2.*");
                    if ($keys)
                    {
                        foreach ($keys as $k)
                        {
                            $redis->del($k);
                        }
                    }
                }
            }

            $redis->del("counter.allpushtime.$k1");
            $redis->del("counter.allpushtime.$k2");
        }

        $redis->close();
        if ($ssdb)$ssdb->close();

        return true;
    }

    /**
     * 更新状态
     */
    public function updateStatus($done = false)
    {
        EtServer::$taskWorkerStatus->set("task{$this->taskId}", ['time' => time(), 'status' => $done ? 0 : 1, 'pid' => $this->server->worker_pid]);
    }

    /**
     * 获取一个任务投递时间
     *
     * @param string $type
     * @return int
     */
    protected static function getJobTime($type)
    {
        # 保存策略（兼顾数据堆积的内存开销和插入频率对性能的影响）:
        # 时间序列为分钟,秒以及无时间分组的, 每分钟保存一次; 其它时间序列每10分钟保存1次
        switch ($type)
        {
            case 'M':      // 分钟
            case 'i':      // 分钟
            case 's':      // 秒
            case 'none':   // none
                # 保存间隔1分钟
                $timeLimit = 60;
                break;

            default:
                # 其它的保存间隔为10分钟
                $timeLimit = 600;
                break;
        }

        return time() + $timeLimit;
    }

    /**
     * 获取Redis连接
     *
     * @return array
     */
    protected static function getRedis()
    {
        try
        {
            $ssdb  = null;
            if (EtServer::$config['redis'][0])
            {
                list ($host, $port) = explode(':', EtServer::$config['redis'][0]);
            }
            else
            {
                $host = EtServer::$config['redis']['host'];
                $port = EtServer::$config['redis']['port'];
            }

            if (EtServer::$config['redis']['hosts'] && count(EtServer::$config['redis']['hosts']) > 1)
            {
                $redis = new RedisCluster(null, EtServer::$config['redis']['hosts']);
            }
            else
            {
                $redis = new redis();

                if (false === $redis->connect($host, $port))
                {
                    throw new Exception('connect redis error');
                }
            }

            if (false === $redis->time(0))
            {
                require_once __DIR__ . '/SSDB.php';
                $ssdb = new SimpleSSDB($host, $port);
            }

            return [$redis, $ssdb];
        }
        catch (Exception $e)
        {
            return [false, false];
        }
    }
}