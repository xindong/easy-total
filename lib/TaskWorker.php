<?php

require_once __DIR__ .'/DataDriver.php';
require_once __DIR__ .'/TaskData.php';

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
     * 数据对象
     *
     * @var TaskData
     */
    protected $taskData;

    /**
     * 记录各个功能的执行时间
     *
     * @var array
     */
    protected $doTime = [];

    public static $serverName;

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

    /**
     * 当程序需要终止时把没处理的任务数据导出到本地
     *
     * @var string
     */
    public static $dumpFile;


    public function __construct(swoole_server $server, $taskId, $workerId)
    {
        $this->server     = $server;
        $this->taskId     = $taskId;
        $this->workerId   = $workerId;
        $this->startTime  = time();
        self::$serverName = EtServer::$config['server']['host'] . ':' . EtServer::$config['server']['port'];

        if ($taskId > 0)
        {
            $serverHash     = substr(md5(EtServer::$configFile), 16, 8);
            self::$dumpFile = EtServer::$config['server']['dump_path'] . 'easy-total-task-dump-' . $serverHash . '-' . $taskId . '.txt';

            TaskData::$dataConfig   = EtServer::$config['data'];
            TaskData::$redisConfig  = EtServer::$config['redis'];
            TaskData::$outputConfig = EtServer::$config['output'];
            $this->taskData         = new TaskData($this->taskId);
        }
    }

    public function init()
    {
        if ($this->taskId > 0)
        {
            $this->loadDumpData();
        }
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
                    $this->taskData->push($data);

                    break;

                case 'job':
                    # 每3秒会被调用1次
                    $this->taskData->run();
                    break;

                case 'clean':
                    # 清理数据, 只有 taskId = 0 的进程会被调用
                    $this->clean();
                    break;

                case 'exit':
                    # 得到进程通知结束
                    $this->shutdown();
                    exit;
                    break;
            }
        }
        catch (Exception $e)
        {
            warn($e->getMessage());
        }

        # 更新任务信息
        $this->updateTaskInfo();

        # 标记状态为成功
        $this->updateStatus(true);

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

    public function shutdown()
    {
        # 将数据保存下来
        $this->dumpData();
    }


    /**
     * 在程序退出时保存数据
     */
    public function dumpData()
    {
        if (TaskData::$jobs)foreach (TaskData::$jobs as $job)
        {
            # 写入到临时数据里, 下次启动时载入
            file_put_contents(self::$dumpFile, 'jobs,'. serialize($job) ."\r\n", FILE_APPEND);
        }

        if (TaskData::$list)foreach (TaskData::$list as $tag => $list)
        {
            file_put_contents(self::$dumpFile, $tag .','. serialize($list) ."\r\n", FILE_APPEND);
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
                if (!$item)continue;

                list($type, $item) = explode(',', $item, 2);
                $tmp  = @unserialize($item);

                if ($tmp)
                {
                    if ($type === 'job')
                    {
                        /**
                         * @var DataJob $tmp
                         */
                        if (isset(TaskData::$jobs[$tmp->uniqueId]))
                        {
                            TaskData::$jobs[$tmp->uniqueId]->merge($tmp);
                        }
                        else
                        {
                            TaskData::$jobs[$tmp->uniqueId] = $tmp;
                        }
                    }
                    else
                    {
                        TaskData::$list[$type] = $tmp;
                    }
                }
            }

            info("Task#$this->taskId reload ". count(TaskData::$jobs) . ' jobs, ' .count(TaskData::$list). ' list from dump file.');

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

    protected function updateTaskInfo()
    {
        # 更新内存占用
        if (!isset($this->doTime['updateMemory']) || time() - $this->doTime['updateMemory'] >= 60)
        {
            list($redis) = self::getRedis();
            $memoryUse   = memory_get_usage(true);
            if ($redis)
            {
                /**
                 * @var Redis $redis
                 */
                $redis->hSet('server.memory', self::$serverName .'_'. $this->workerId, serialize([$memoryUse, time(), self::$serverName, $this->workerId]));
            }
            $this->doTime['updateMemory'] = time();

            info("Task". str_pad('#'.$this->taskId, 4, ' ', STR_PAD_LEFT) ." total jobs: ". count(TaskData::$jobs) .", memory: ". number_format($memoryUse/1024/1024, 2) ."MB.");
        }
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