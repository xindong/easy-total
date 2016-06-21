<?php
/**
 * 任务数据处理对象
 */
class TaskData
{
    /**
     * 任务进程ID
     *
     * @var int
     */
    public $taskId;

    /**
     * 进程ID
     *
     * @var int
     */
    public $workerId;

    /**
     * 任务进程ID
     *
     * @var int
     */
    public $workerPid;

    /**
     * 任务数据
     *
     * @var array
     */
    public $jobs = [];

    /**
     * 任务的缓存数据
     *
     * @var array
     */
    protected $jobsCache = [];

    /**
     * @var DataDriver
     */
    protected $driver;

    /**
     * 任务列表
     *
     * @var array
     */
    protected $processes = [];

    public static $dataConfig = [];

    public static $redisConfig;

    /**
     * @var array
     */
    public static $queries = [];

    /**
     * 序列的设置对象
     *
     * @var array
     */
    public static $series = [];

    /**
     * 输出的配置
     *
     * @var array
     */
    public static $outputConfig = [];

    public function __construct($taskId, $workerId, $workerPid)
    {
        $this->taskId    = $taskId;
        $this->workerId  = $workerId;
        $this->workerPid = $workerPid;
        $this->driver    = new DataDriver(self::$dataConfig);

        # 创建推送数据的进程
        for ($i = 0; $i < 3; $i++)
        {
            $process = new TaskProcess($taskId, $i);
            $process->start($this);
            $this->processes[] = $process;
        }
    }

    /**
     * 添加一个新的任务数据
     *
     * @param DataJob $job
     * @return bool
     */
    public function pushJob(DataJob $job)
    {
        if (isset($this->jobs[$job->uniqueId]))
        {
            # 合并数据
            $this->jobs[$job->uniqueId]->merge($job);
        }
        elseif (isset($this->jobsCache[$job->uniqueId]))
        {
            # 用缓存中数据
            $this->jobs[$job->uniqueId] = $this->jobsCache[$job->uniqueId];
            $this->jobs[$job->uniqueId]->merge($job);
        }
        elseif ($job->total->loadFromDB)
        {
            # 充数据中加载的
            $this->jobs[$job->uniqueId] = $job;
        }
        else
        {
            # 加载旧数据
            $oldJob = $this->driver->getTotal($job->uniqueId);
            if (false)return false;

            # 合并统计
            $job->mergeTotal($oldJob);

            # 设置对象
            $this->jobs[$job->uniqueId] = $job;
        }

        return true;
    }

    /**
     * 导出数据
     *
     * @param int $taskWorkerId
     */
    public function run()
    {
        if ($this->jobs)
        {
            if ($count = count($this->jobsCache) > 50000)
            {
                # 如果数据太多则清理下
                $this->jobsCache = array_slice($this->jobsCache, -5000, null, true);

                debug("clean jobs cache, count: $count");
            }

            foreach ($this->jobs as $job)
            {
                /**
                 * @var DataJob $job
                 */
                if ($job->dist)
                {
                    # 有唯一序列数据, 保存唯一数据
                    if (!$this->save($job))
                    {
                        # 保存失败, 处理下一个
                        continue;
                    }
                }

                # 导出到列表
                if ($this->exportToListByJob($job))
                {
                    # 移到任务缓存数组里
                    $this->jobsCache[$job->uniqueId] = $job;

                    # 从当前任务中移除
                    unset($this->jobs[$job->uniqueId]);
                }

                $this->updateStatus();
            }
        }
    }

    /**
     * 更新状态
     */
    protected function updateStatus()
    {
        EtServer::$taskWorkerStatus->set("task{$this->taskId}", ['time' => time(), 'status' => 1, 'pid' => $this->workerPid]);
    }

    /**
     * 保存数据
     *
     * @param $uniqueId
     * @return bool
     */
    protected function save(DataJob $job)
    {
        foreach ($job->dist as $field => $v)
        {
            if ($count = $this->driver->saveDist($job->uniqueId, $field, $v))
            {
                $job->total->dist[$field] = $count;

                # 保存成功后, 移除列表内容释放内存
                unset($job->dist[$field]);
            }
            else
            {
                return false;
            }
        }
        unset($field, $v);

        # 保存统计数据
        $rs = $this->driver->saveTotal($job->uniqueId, $job->total);

        if ($rs)
        {
            $job->saved = true;
        }

        return $rs;
    }

    /**
     * 导出列表数据
     *
     * @param $id
     * @param $listData
     * @return bool
     */
    public function exportToListByJob(DataJob $job)
    {
        $seriesKey = $job->seriesKey;

        if (!isset(self::$series[$seriesKey]))
        {
            # 没有对应的序列
            $redis = self::getRedis();
            if ($redis)
            {
                self::$series = array_map('unserialize', $redis->hGetAll('series'));
            }
            else
            {
                return false;
            }

            if (!self::$series[$seriesKey])
            {
                # 重新获取后序列还不存在
                return true;
            }
        }

        $seriesOption = self::$series[$seriesKey];
        $queries      = $seriesOption['queries'] ?: [];

        if ($job->timeOpType === 'none')
        {
            $timeOptKey = 'none';
        }
        else
        {
            $timeOptKey = $job->timeOpLimit . $job->timeOpType;
        }

        if (isset($queries[$timeOptKey]))
        {
            foreach ($queries[$timeOptKey] as $queryKey)
            {
                if (!isset(self::$queries[$queryKey]))
                {
                    if (!isset($redis))
                    {
                        $redis = self::getRedis();
                        if (false === $redis)return false;
                    }

                    self::$queries = array_map('unserialize', $redis->hGetAll('queries'));
                }

                $queryOption = self::$queries[$queryKey];
                if (!$queryOption)
                {
                    # 没有对应的查询
                    continue;
                }

                # 查询已经更改
                if ($queryOption['seriesKey'] !== $seriesKey)continue;

                # 生成数据
                $data = [
                    '_id'    => $job->dataId,
                    '_group' => $job->timeKey,
                ];

                if ($queryOption['allField'])
                {
                    $data += $job->data;
                }

                # 排除字段
                if (isset($queryOption['function']['exclude']))
                {
                    # 示例: select *, exclude(test), exclude(abc) from ...
                    foreach ($queryOption['function']['exclude'] as $field => $t)
                    {
                        unset($data[$field]);
                    }
                }

                try
                {
                    foreach ($queryOption['fields'] as $as => $saveOpt)
                    {
                        $field = $saveOpt['field'];
                        $type  = $saveOpt['type'];
                        switch ($type)
                        {
                            case 'count':
                            case 'sum':
                            case 'min':
                            case 'max':
                            case 'dist':
                                $data[$as] = $job->total->$type[$field];
                                break;

                            case 'first':
                            case 'last':
                                $data[$as] = $job->total->$type[$field][0];
                                break;

                            case 'avg':
                                # 平均数
                                $sum   = $job->total->sum[$field];
                                $count = $job->total->count['*'];
                                if ($count > 0)
                                {
                                    $data[$as] = $sum / $count;
                                }
                                else
                                {
                                    $data[$as] = 0;
                                }
                                break;

                            case 'value':
                                if (isset($item[$field]))
                                {
                                    # 没设置的不需要赋值
                                    $data[$as] = $item[$field];
                                }
                                break;
                        }
                    }
                }
                catch(Exception $e)
                {
                    warn($e->getMessage());
                    continue;
                }

                # 导出数据的KEY
                if (is_array($queryOption['saveAs'][$timeOptKey]))
                {
                    $tmp = $queryOption['saveAs'][$timeOptKey];
                    switch ($tmp[1])
                    {
                        case 'date':
                            # 处理时间变量替换
                            $saveAs = str_replace($tmp[2], explode(',', date($tmp[3], $job->time)), $tmp[0]);
                            break;

                        default:
                            $saveAs = $tmp[0];
                            break;
                    }
                    unset($tmp);
                }
                else
                {
                    $saveAs = $queryOption['saveAs'][$timeOptKey];
                }

                # 拼接 tag
                $tag = self::$outputConfig['prefix'] ."$job->app.$saveAs";

                # 导出列表数据列
                $str = $tag .','. json_encode([$job->time, $data], JSON_UNESCAPED_UNICODE);

                /**
                 * @var swoole_process $process
                 */
                $process = $this->processes[crc32($tag) % 3];

                if (!$process->push($str))
                {
                    # 发送失败
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 任务进程退出时调用
     */
    public function shutdown()
    {
        $pids = [];
        foreach ($this->processes as $process)
        {
            /**
             * @var swoole_process $process
             */
            $pids[] = $process->pid;

            # 发送一个退出的信号
            swoole_process::kill($process->pid);
        }

        $time = time() + 1;
        while($pids)
        {
            sleep(1);
            while(swoole_process::wait(false));

            foreach ($pids as $k => $pid)
            {
                if (in_array($pid, $rs = explode("\n", str_replace(' ', '', trim(`ps -eopid | grep {$pid}`)))))
                {
                    warn("task $this->taskId process is running, have been waiting for " . (time() - $time) . "s.");

                    if (time() - $time > 60)
                    {
                        # 超过1分钟还没有结束, 强制关闭
                        swoole_process::kill($pid, -9);
                        unset($pids[$k]);
                    }
                }
                else
                {
                    unset($pids[$k]);
                }
            }
        }
    }

    /**
     * 创建任务进程
     */
    public static function clean(TaskData $dataJob)
    {
        $dataJob->jobs      = [];
        $dataJob->jobsCache = [];
        $dataJob->processes = [];
        $dataJob->driver    = null;
    }

    /**
     * 获取Redis连接
     *
     * @return Redis
     */
    protected static function getRedis()
    {
        try
        {
            $ssdb  = null;
            if (self::$redisConfig[0])
            {
                list ($host, $port) = explode(':', self::$redisConfig[0]);
            }
            else
            {
                $host = self::$redisConfig['host'];
                $port = self::$redisConfig['port'];
            }

            if (self::$redisConfig['hosts'] && count(self::$redisConfig['hosts']) > 1)
            {
                $redis = new RedisCluster(null, self::$redisConfig['hosts']);
            }
            else
            {
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
}
