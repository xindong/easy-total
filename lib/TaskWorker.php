<?php

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
    protected $id;

    /**
     * @var swoole_server
     */
    protected $server;

    /**
     * 当程序需要终止时如果无法把数据推送出去时临时导出的文件路径（确保数据安全）
     *
     * @var string
     */
    protected $dumpFile;

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
     * @var array
     */
    protected $taskThreaded = [];

    protected $autoPause = false;

    /**
     * @var array
     */
    public static $dist = [];
    /**
     * @var array
     */
    public static $total = [];
    /**
     * @var array
     */
    public static $jobs = [];

    /**
     * 分配任务执行时间
     *
     * @var array
     */
    public static $jobTime = [];

    public static $serverName;

    /**
     * 单个任务最多积压的任务数
     *
     * @var int
     */
    public static $maxJobs = 10000;

    public function __construct(swoole_server $server, $id, $workerId)
    {
        require_once __DIR__ .'/TaskData.php';
        include_once __DIR__ .'/DataDriver.php';

        $this->server    = $server;
        $this->taskId    = $id;
        $this->id        = $workerId;
        $this->dumpFile  = EtServer::$config['server']['dump_path'] . 'total-task-dump-'. substr(md5(EtServer::$configFile), 16, 8) . '-'. $id .'.txt';
        $this->startTime = time();

        # 更新状态
        $this->updateStatus(1);

        # 设置配置
        self::$serverName           = EtServer::$config['server']['host'] .':'. EtServer::$config['server']['port'];
        self::$maxJobs              = EtServer::$config['server']['max_jobs'] ?: 10000;
        TaskThreaded::$distConfig   = EtServer::$config['dist'];
        TaskThreaded::$redisConfig  = EtServer::$config['redis'];
        TaskThreaded::$outputConfig = EtServer::$config['output'];
        TaskThreaded::$dumpPath     = EtServer::$config['server']['dump_path'];
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
        usleep(10000 * $this->taskId);

        try
        {
            if (is_object($data))
            {
                # 获取数据类型
                $type = get_class($data);
            }
            elseif (is_array($data))
            {
                $type = 'list';
            }
            else
            {
                $data = explode('|', $data);
                $type = $data[0];
            }

            switch ($type)
            {
                case 'DataDist':
                    # 唯一数
                    foreach ($data as $uniqueId => $value)
                    {
                        if (isset(self::$dist[$uniqueId]))
                        {
                            foreach ($value as $field => $items)
                            {
                                if (isset(self::$dist[$uniqueId][$field]))
                                {
                                    self::$dist[$uniqueId][$field] = array_merge(self::$dist[$uniqueId][$field], $items);
                                }
                                else
                                {
                                    self::$dist[$uniqueId][$field] = $items;
                                }
                            }
                        }
                        else
                        {
                            self::$dist[$uniqueId] = $value;
                        }

                        if (!isset(self::$jobTime[$uniqueId]))
                        {
                            self::setJobTime($uniqueId);
                        }
                    }

                    break;

                case 'DataTotal':
                    # 合并统计数
                    foreach ($data as $uniqueId => $value)
                    {
                        if (!isset(self::$total[$uniqueId]) || !self::$total[$uniqueId]->lastLoadTime)
                        {
                            if (!isset($driver))
                            {
                                $driver = new DataDriver(TaskThreaded::$distConfig);
                            }

                            # 在以前记录的数据里获取历史统计数据
                            $rs = $driver->getTotal($uniqueId);

                            if ($rs)
                            {
                                if (!isset(self::$total[$uniqueId]))
                                {
                                    self::$total[$uniqueId] = $rs;
                                }
                                else
                                {
                                    # 数据合并
                                    TaskThreaded::totalDataMerge(self::$total[$uniqueId], $rs);
                                }

                                # 更新加载时间
                                self::$total[$uniqueId]->lastLoadTime = time();
                            }
                        }

                        # 合并已存在的统计数据
                        TaskThreaded::totalDataMerge(self::$total[$uniqueId], $value);

                        if (!isset(self::$jobTime[$uniqueId]))
                        {
                            self::setJobTime($uniqueId);
                        }
                    }
                    break;

                case 'DataJobs':
                    # 任务数据
                    foreach ($data as $uniqueId => $value)
                    {
                        if (isset(self::$jobs[$uniqueId]))
                        {
                            foreach ($value as $k => $v)
                            {
                                self::$jobs[$uniqueId][$k] = $v;
                            }
                        }
                        else
                        {
                            self::$jobs[$uniqueId] = $value;
                        }

                        if (!isset(self::$jobTime[$uniqueId]))
                        {
                            self::setJobTime($uniqueId);
                        }
                    }

                    # 当前任务数
                    if (!$this->autoPause && ($count = count(self::$jobs)) > self::$maxJobs)
                    {
                        # 积压的任务数非常多
                        debug("task $this->id jobs num: $count");
                        info("the jobs is too much, now notify server pause accept new data.");
                        $this->notifyWorkerPause();
                    }
                    break;

                case 'job':
                    # 每3秒会被调用1次
                    $this->saveData();
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
        if (!isset($doTime['updateMemory']) || time() - $doTime['updateMemory'] >= 60)
        {
            list($redis) = self::getRedis();
            if ($redis)
            {
                /**
                 * @var Redis $redis
                 */
                $redis->hSet('server.memory', self::$serverName .'_'. $this->id, serialize([memory_get_usage(true), time(), self::$serverName, $this->id]));
            }
            $doTime['updateMemory'] = time();
        }

        # 标记状态为成功
        $this->updateStatus(1);

        # 任务数小余一定程度后继续执行
        if ($this->autoPause)
        {
            if (count(self::$jobs) < self::$maxJobs / 10)
            {
                info("now notify server continue accept new data.");
                $this->notifyWorkerContinue();
            }
        }

        # 如果启动超过1小时
        if (time() - $this->startTime > 3600)
        {
            if (mt_rand(1, 200) === 1)
            {
                # 重启进程避免数据溢出、未清理数据占用超大内存
                $this->shutdown();

                info('now restart task worker: '. $this->taskId);

                exit(0);
            }
        }

//        usleep($this->taskId * 100);
//        echo "================={$this->taskId}\n";
//        if (self::$dist)
//        {
//            echo 'self::$dist=';
//            print_r(self::$dist);
//        }
//        if (self::$total)
//        {
//            echo 'self::$total=';
//            print_r(self::$total);
//        }
//        if (self::$jobs)
//        {
//            echo 'self::$jobs=';
//            print_r(self::$jobs);
//        }
//        if (self::$jobs)
//        {
//            echo 'self::$jobTime=';
//            print_r(self::$jobTime);
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
     * 保存数据
     */
    public function saveData()
    {
        if ($this->taskThreaded)
        {
            $this->checkTaskThreaded();

            # 最对3个线程（进程）同时处理
            if (count($this->taskThreaded) >= 3)
            {
                return;
            }
        }

        $now  = time();
        # 构造一个新的任务对象（支持多线程或多进程）
        $task = new TaskThreaded();
        # 设置当前任务的id
        $task->taskId = $this->taskId;

        # 获取需要处理的任务
        foreach (self::$jobTime as $uniqueId => $time)
        {
            echo ($now - $time);
            if ($now >= $time)
            {
                # 到达执行任务的时间了
                if (isset(self::$dist[$uniqueId]))
                {
                    $task->dist[$uniqueId] = self::$dist[$uniqueId];
                    unset(self::$dist[$uniqueId]);
                }

                if (isset(self::$jobs[$uniqueId]))
                {
                    $task->jobs[$uniqueId] = self::$jobs[$uniqueId];
                    unset(self::$jobs[$uniqueId]);
                }

                if (!isset(self::$total[$uniqueId]))
                {
                    self::$total[$uniqueId] = new DataTotalItem();
                }

                # 任务统计数值
                $task->total[$uniqueId] = self::$total[$uniqueId];

                # 移除任务时间
                unset(self::$jobTime[$uniqueId]);
            }
        }

        if (count($task->jobs) || count($task->dist))
        {
            # 有任务数据
            $this->taskThreaded[] = $task;

            # 执行
            $task->start();
        }
    }


    protected function checkTaskThreaded()
    {
        $c = false;
        foreach ($this->taskThreaded as $key => $item)
        {
            /**
             * @var DataThreaded $item
             */
            if ($item->isRunning())
            {
                if (time() - $item->getLastRunTime() > 300)
                {
                    # 一直更新的执行时间超过 5 分钟, 可能是进程死掉了
                    warn("task process has been dead more than 10 minutes, now kill it.");
                    $item->kill();

                    unset($this->taskThreaded[$key]);
                    $c = true;
                }
            }
            else
            {
                $item->close();
                unset($this->taskThreaded[$key]);
                $c = true;
            }
        }

        if ($c)
        {
            # 整理数组
            $this->taskThreaded = array_values($this->taskThreaded);
        }
    }


    public function shutdown()
    {
        # 将数据保存下来
        $this->dumpData();

        # 清空数据
        self::$jobTime = [];
        self::$dist    = [];
        self::$jobs    = [];

        if ($this->taskThreaded)
        {
            $time = time();
            # 有跑着的任务
            while ($this->taskThreaded)
            {
                warn("task $this->taskId have ". count($this->taskThreaded) ." process is running, have been waiting for ". (time() - $time) ."s.");

                # 检查任务状态
                $this->checkTaskThreaded();

                if (time() - $time > 300)
                {
                    # 超过5分钟还没有处理完毕, 通知进程 dump 数据
                    foreach ($this->taskThreaded as $item)
                    {
                        /**
                         * @var TaskThreaded $item
                         */
                        $item->dump();
                    }
                    break;
                }

                sleep(1);
            }
        }
    }


    /**
     * 在程序退出时保存数据
     */
    public function dumpData()
    {
        if (self::$jobTime || self::$dist || self::$jobs)
        {
            $dump = [
                'jobTime' => self::$jobTime,
                'dist'    => self::$dist,
                'jobs'    => self::$jobs,
            ];

            # 写入到临时数据里, 下次启动时载入
            file_put_contents($this->dumpFile, serialize($dump));
        }
    }

    protected function loadDumpData()
    {
        if (is_file($this->dumpFile))
        {
            $tmp = @unserialize(file_get_contents($this->dumpFile));
            if ($tmp && is_array($tmp))
            {
                self::$jobTime = $tmp['jobTime'];
                self::$dist    = $tmp['dist'];
                self::$jobs    = $tmp['jobs'];
            }

            unlink($this->dumpFile);
        }

        # 读取子进（线）程dump出的数据
        $dumpFile = EtServer::$config['server']['dump_path'] . 'total-task-process-dump-' . $this->taskId . '.txt';
        if (is_file($dumpFile))
        {
            foreach (explode("\r\n", rtrim(file_get_contents($dumpFile))) as $item)
            {
                $tmp = @unserialize($item);
                if ($tmp && is_array($tmp))
                {
                    $task = new TaskThreaded();
                    $task->taskId = $this->taskId;
                    $task->restore($tmp);

                    $this->taskThreaded[] = $task;
                }

                ////////////debug
                break;
            }
            unlink($dumpFile);
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
        $this->updateStatus();

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
                    $this->updateStatus();
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
     *
     * @param int $status 1 - 成功, 2 - 运行中
     */
    public function updateStatus($status = 2)
    {
        EtServer::$taskWorkerStatus->set("task{$this->taskId}", ['status' => $status, 'time' => time()]);
    }

    protected static function setJobTime($uniqueId)
    {
        # $key = abcde123af32,1d,hsqj,2016001,123_abc
        list($seriesKey, $timeOptKey) = explode(',', $uniqueId, 3);

        # 保存策略（兼顾数据堆积的内存开销和插入频率对性能的影响）:
        # 时间序列为分钟,秒以及无时间分组的, 每分钟保存一次; 其它时间序列每10分钟保存1次
        switch (substr($timeOptKey, -1))
        {
            case 'M':   // 分钟
            case 'i':   // 分钟
            case 's':   // 秒
            case 'e':   // none
                # 保存间隔1分钟
                $timeLimit = 60;
                break;

            default:
                # 其它的保存间隔为10分钟
                $timeLimit = 600;
                break;
        }

        # 设定下一个任务时间
        # 按1分钟分组, 并且向后延 60 或 600 秒, 这样可以把跨度较长的任务时间分割开
        self::$jobTime[$uniqueId] = intval(time() / 60) * 60 + $timeLimit;
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