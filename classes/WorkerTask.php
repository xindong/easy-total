<?php

class WorkerTask extends MyQEE\Server\WorkerTask
{
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

    public static $timed;

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
    protected static $dumpFile;

    /**
     * 当重新临时重启时dump的目录（一般为内存目录）
     *
     * @var string
     */
    protected static $reloadDumpFile;

    public function onStart()
    {
        if ($this->taskId > 0)
        {
            $serverHash             = substr(md5(EtServer::$configFile), 16, 8);
            self::$dumpFile         = EtServer::$config['server']['dump_path'] . 'easy-total-task-dump-' . $serverHash . '-' . $this->taskId . '.txt';
            self::$reloadDumpFile   = EtServer::$config['conf']['task_tmpdir'] . 'easy-total-task-dump-' . $serverHash . '-' . $this->taskId . '.txt';
            TaskData::$dataConfig   = EtServer::$config['data'];
            TaskData::$redisConfig  = EtServer::$config['redis'];
            TaskData::$outputConfig = EtServer::$config['output'];
            $this->taskData         = new TaskData($this->taskId);

            $this->loadDumpData();

            # 每3秒处理1次
            $this->timeTick(3000, function()
            {
                self::$timed = time();
                $this->taskData->run();
            });

            # 每分钟更新任务的信息
            $this->timeTick(60000, function()
            {
                $this->updateTaskInfo();
            });
        }
        else
        {
            # 定时数据清理
            swoole_timer_tick(1000 * 60 * 5, function()
            {
                $this->clean();
            });
        }

        # 每1-2小时里重启进程避免数据溢出、未清理数据占用超大内存
        swoole_timer_tick(mt_rand(1000 * 3600, 1000 * 3600 * 2), function()
        {
            $this->dumpData(true);

            $this->info("Task#$this->taskId now auto restart.");

            exit(0);
        });
    }

    /**
     * @param \Swoole\Server $server
     * @param $taskId
     * @param $fromId
     * @param $data
     * @return mixed
     */
    public function onTask($server, $taskId, $fromId, $data, $fromServerId = -1)
    {
        try
        {
            self::$timed = time();

            if (is_object($data))
            {
                # 获取数据类型
                $type = get_class($data);
            }
            else
            {
                $data = explode('|', $data, 2);
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

                case 'shm':
                    # 收到一个内存投递的请求
                    $this->loadDataByShmId($data[1]);
                    break;

                case 'total':
                    # 获取指定任务实时统计数据
                    list($queryKey, $uniqueId) = explode('|', $data[1], 3);

                    return $this->taskData->getRealTimeData($uniqueId, $queryKey);

                default:
                    $this->warn("unknown task type $type");
                    break;
            }
        }
        catch (Exception $e)
        {
            $this->warn($e->getMessage());
        }

        return null;
    }

    /**
     * 通过共享内存来读取数据
     *
     * todo 还没有测试过可靠性如何
     *
     * @param $shmKey
     */
    protected function loadDataByShmId($shmKey)
    {
        $shmId = @shmop_open($shmKey, 'w', 0664, 0);

        if ($shmId)
        {
            # 读取记录的成功的位置
            $pos = rtrim(@shmop_read($shmId, 0, 8), "\0");
            if ($pos)
            {
                $pos = unpack('J', $pos);
                if ($pos)
                {
                    if (is_array($pos))
                    {
                        $pos = $pos[1];
                    }

                    if ($pos < 8)$pos = 8;
                }
                else
                {
                    $pos = 8;
                }
            }
            else
            {
                $pos = 8;
            }

            $rs    = '';
            $str   = '';
            $size  = shmop_size($shmId);
            $limit = 4096;

            if ($limit + $pos >= $size)
            {
                # 如果超过内存大小则缩小读取量
                $limit = $size - $pos;
            }

            # 读取共享内存数据
            if ($limit > 0)while ($rs = @shmop_read($shmId, $pos, $limit))
            {
                $data = rtrim($rs, "\0");
                $pos += $limit;
                $tmp  = $str . $data;

                if (false !== strpos($tmp, "\1\r\n"))
                {
                    $arr = explode("\1\r\n", $tmp);
                    foreach ($arr as $item)
                    {
                        if ($item === '')continue;

                        # 解开数据
                        $dataJob = @msgpack_unpack($item);
                        if ($dataJob && $dataJob instanceof DataJob)
                        {
                            # 合并任务
                            $this->taskData->push($dataJob);

                            # 写入当前成功的位置
                            @shmop_write($shmId, pack('J', $pos), 0);
                        }
                        else
                        {
                            # 记录到 $str 里继续读取
                            $str = $item;
                        }
                    }
                }
                else
                {
                    $str .= $data;
                }

                if ($pos + $limit > $size)
                {
                    $limit = $size - $pos;

                    # 已经到结尾了
                    if ($limit <= 0)break;
                }
            }

            if ($rs !== false)
            {
                # 移除数据
                shmop_delete($shmId);
                shmop_close($shmId);
            }
        }
        else
        {
            $this->warn("can not open shm, shm key is : $shmKey");
        }
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
     *
     * @param bool $isReload 是否重新加载方式
     */
    public function dumpData($isReloadMod = false)
    {
        if ($isReloadMod)
        {
            $file = self::$reloadDumpFile;
        }
        else
        {
            $file = self::$dumpFile;
        }
        if (!$file)return;

        $this->info("Task#$this->taskId is dumping file.");

        $time = microtime(1);
        if (TaskData::$jobs)foreach (TaskData::$jobs as $jobs)
        {
            # 写入到临时数据里, 下次启动时载入
            foreach ($jobs as $job)
            {
                /**
                 * @var DataJob $job
                 */
                if ($job->total->all)
                {
                    # 尝试将数据保存到统计汇总里
                    $this->taskData->saveJob($job);
                }
                file_put_contents($file, 'jobs,' . msgpack_pack($job) . "\0\r\n", FILE_APPEND);
            }
        }

        if (TaskData::$list)foreach (TaskData::$list as $tag => $list)
        {
            file_put_contents($file, $tag .','. msgpack_pack($list) ."\0\r\n", FILE_APPEND);
        }

        $this->info("Task#$this->taskId dump job: ". TaskData::getJobCount() .". list: ". count(TaskData::$list) .", use time:". (microtime(1) - $time) ."s.");
    }

    /**
     * 加载dump出去的数据
     */
    protected function loadDumpData()
    {
        if (self::$dumpFile || self::$reloadDumpFile)
        {
            if (self::$dumpFile)
            {
                # 系统重启的dump路径
                $this->loadDumpDataFromFile(self::$dumpFile);
            }

            if (self::$reloadDumpFile)
            {
                # 进程释放内存临时重启dump的路径
                $this->loadDumpDataFromFile(self::$reloadDumpFile);
            }

            $this->info("Task#$this->taskId load " . TaskData::getJobCount() . " jobs, " . count(TaskData::$list) . ' list from dump file.');
        }

        # 只需要在第一个任务进程执行
        if ($this->taskId === 1)
        {
            # 如果调小过 task worker num, 需要把之前的 dump 的数据重新 load 回来
            $files = preg_replace('#\-'. $this->taskId .'\.txt$#', '-*.txt', self::$dumpFile);

            # 所有任务数减1则为最大任务数的序号
            $maxIndex = $this->server->setting['task_worker_num'] - 1;
            foreach (glob($files) as $file)
            {
                if (preg_match('#\-(\d+)\.txt$#', $file, $m))
                {
                    if ($m[1] > $maxIndex)
                    {
                        # 序号大于最大序号
                        $this->loadDumpDataFromFile($file);
                    }
                }
            }
        }
    }

    protected function loadDumpDataFromFile($file)
    {
        if (is_file($file))
        {
            foreach (explode("\0\r\n", file_get_contents($file)) as $item)
            {
                if (!$item)continue;

                list($type, $tmp) = explode(',', $item, 2);

                $tmp = @msgpack_unpack($tmp);
                if ($tmp)
                {
                    if ($type === 'jobs')
                    {
                        if (is_object($tmp) && $tmp instanceof DataJob)
                        {
                            $this->taskData->push($tmp, true);
                        }
                    }
                    else
                    {
                        if (is_array($tmp))
                        {
                            TaskData::$list[$type] = $tmp;
                        }
                    }
                }
            }

            unlink($file);
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
                $this->info("clean data, remove sql({$key}): {$query['sql']}");
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
                }
            }
        }


        # 清理每天的统计数据, 只保留10天内的
        $time = time();
        $k1   = date('Ymd', $time - 86400 * 12);
        $k2   = date('Ymd', $time - 86400 * 11);

        if ($ssdb)
        {
            foreach (['total', 'time'] as $item)
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

            $ssdb->hclear("counter.flush.time.$k1");
            $ssdb->hclear("counter.flush.time.$k2");
        }
        else
        {
            foreach (['total', 'time'] as $item)
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

            $redis->del("counter.flush.time.$k1");
            $redis->del("counter.flush.time.$k2");
        }

        $redis->close();
        if ($ssdb)$ssdb->close();

        return true;
    }

    protected function updateTaskInfo()
    {
        # 更新内存占用
        list($redis) = self::getRedis();
        $memoryUse   = memory_get_usage(true);
        if ($redis)
        {
            /**
             * @var Redis $redis
             */
            $redis->hSet('server.memory', self::$serverName .'_'. $this->id, serialize([$memoryUse, time(), self::$serverName, $this->id]));
        }
        $this->doTime['updateMemory'] = time();

        $this->info("Task". str_pad('#'.$this->taskId, 4, ' ', STR_PAD_LEFT) ." total jobs: ". TaskData::getJobCount() .", memory: ". number_format($memoryUse/1024/1024, 2) ."MB.");
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