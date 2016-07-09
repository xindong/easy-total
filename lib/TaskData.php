<?php


/**
 * 任务数据处理对象
 *
 * Class TaskData
 */
class TaskData
{
    /**
     * 任务进程ID
     *
     * @var int
     */
    protected $taskId;

    /**
     * 数据存储驱动对象
     *
     * @var DataDriver
     */
    protected $driver;

    /**
     * 当前执行的时间
     *
     * @var int
     */
    protected $runTime;

    protected $doTime = [];

    /**
     * 所有任务列表
     *
     * @var array
     */
    public static $jobs = [];

    /**
     * 任务的缓存数据
     *
     * @var array
     */
    public static $jobsCache = [];

    /**
     * 导出的列表数据
     *
     * @var array
     */
    public static $list = [];

    /**
     * 数据配置
     *
     * @var array
     */
    public static $dataConfig = [];

    /**
     * redis配置
     *
     * @var array
     */
    public static $redisConfig = [];

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

    /**
     * 发送数据的列队
     *
     * @var array
     */
    public static $sendEvents = [];

    /**
     * JobData constructor.
     */
    public function __construct($taskId)
    {
        $this->taskId = $taskId;
        $this->driver = new DataDriver(self::$dataConfig);
    }

    /**
     * 执行
     *
     * 每3秒会被task进程调1次, 所以一般情况下运行时间不超过 3 秒
     */
    public function run()
    {
        $this->runTime = microtime(1);

        # 任务数据处理
        $this->export();

        # 导出数据
        $this->output();

        if ($count = count(self::$jobsCache) > 50000)
        {
            # 如果数据太多则清理下
            self::$jobsCache = array_slice(self::$jobsCache, -5000, null, true);

            debug("Task#$this->taskId clean jobs cache, count: $count");
        }
    }

    /**
     * 添加数据
     *
     * @param DataJob $job
     */
    public function push(DataJob $job)
    {
        $uniqueId  = $job->uniqueId;
        $seriesKey = $job->seriesKey;

        if (!isset(self::$jobs[$seriesKey]))
        {
            self::$jobs[$seriesKey] = new ArrayObject();
        }

        # 当前对象列表
        $jobs = self::$jobs[$seriesKey];

        if (isset($jobs[$uniqueId]))
        {
            # 合并任务
            $jobs[$uniqueId]->merge($job);
        }
        else
        {
            if (isset(self::$jobsCache[$uniqueId]))
            {
                $jobs[$uniqueId] = self::$jobsCache[$uniqueId];
                
                # 将 $job 合并到当前缓存对象里
                $jobs[$uniqueId]->merge($job);

                # 重新赋值
                $job = $jobs[$uniqueId];
            }
            else
            {
                # 加入列表
                $jobs[$uniqueId] = $job;
            }

            # 设置投递时间
            $job->taskTime = TaskWorker::$timed + self::getDelayTime($job);
        }
    }

    /**
     * 更新状态
     */
    public function updateStatus()
    {
        EtServer::$taskWorkerStatus->set("task{$this->taskId}", ['time' => time(), 'status' => 1]);
    }

    /**
     * 导出数据
     */
    protected function export()
    {
        $success = 0;
        $fail    = 0;
        $count   = 0;
        $time    = microtime(1);

        foreach (self::$jobs as $jobs)
        {
            $this->exportByList($jobs, $success, $fail);

            $count += count($jobs);
        }
        $useTime = microtime(1) - $time;

        if (IS_DEBUG)
        {
            if ($success || $fail)
            {
                debug("Task#$this->taskId export data: $success" . ($fail ? ", fail: $fail." : '.') . "use time: $useTime, now jobs count: $count.");
            }
        }
    }

    protected function exportByList(& $jobs, & $success, & $fail)
    {
        foreach ($jobs as $job)
        {
            /**
             * @var DataJob $job
             */
            if ($this->runTime < $job->taskTime)
            {
                # 还没到任务处理时间, 后面的数据肯定也没有到, 直接跳出
                break;
            }

            if (!$job->total->all)
            {
                # 需要加载数据
                $oldJob = $this->driver->getTotal($job->uniqueId);
                if (!$oldJob)
                {
                    # 加载旧数据失败, 处理下一个
                    continue;
                }

                # 合并统计
                $job->mergeTotal($oldJob);
            }

            if ($job->dist)
            {
                # 有唯一序列数据, 保存唯一数据
                if (!$this->saveJob($job))
                {
                    # 保存失败, 处理下一个
                    continue;
                }
            }

            # 导出到列表
            if ($this->exportToListByJob($job))
            {
                # 移到任务缓存数组里
                self::$jobsCache[$job->uniqueId] = $job;

                # 从当前任务中移除
                unset($jobs[$job->uniqueId]);
                $success++;
            }
            else
            {
                $fail++;
            }

            if ($success % 1000 === 0)
            {
                $this->updateStatus();
            }
        }
    }

    /**
     * 保存数据
     *
     * @param $uniqueId
     * @return bool
     */
    protected function saveJob(DataJob $job)
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

        if ($job->timeOpType === '-')
        {
            $timeOptKey = '-';
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
                        if (false === $redis)
                        {
                            return false;
                        }
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
                if ($queryOption['seriesKey'] !== $seriesKey)
                {
                    continue;
                }

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
                catch (Exception $e)
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
                $tag = self::$outputConfig['prefix'] . "$job->app.$saveAs";

                # 导出列表数据列
                self::$list[$tag][$job->dataId] = json_encode([$job->time, $data], JSON_UNESCAPED_UNICODE);
            }
        }

        return true;
    }

    /**
     * 将数据发送到 Fluent
     */
    protected function output()
    {
        try
        {
            output:
            if (self::$sendEvents)
            {
                $this->checkAck();
            }

            foreach (self::$list as $tag => & $item)
            {
                # 发送数据

                # 如果有相同的tag的发送的列队, 则跳过
                if (isset(self::$sendEvents[$tag]))
                {
                    continue;
                }

                $rs = self::sendToFluent($tag, $item);
                if (true === $rs)
                {
                    # 发送成功, 移除数据
                    unset(self::$list[$tag]);
                }
                elseif (false === $rs)
                {
                    warn("push data {$tag} fail. fluent server: " . self::$outputConfig['type'] . ': ' . self::$outputConfig['link']);
                }

                $this->updateStatus();
            }
        }
        catch (Exception $e)
        {
            warn($e->getMessage());

            if (IS_DEBUG)
            {
                echo $e->getTraceAsString();
            }
        }
    }

    /**
     * 检查ACK返回
     */
    protected function checkAck()
    {
        $i    = 0;
        $time = microtime(1);

        if (IS_DEBUG)
        {
            static $success = 0;
            static $fail = 0;
        }
        else
        {
            $success = 0;
            $fail    = 0;
        }

        foreach (self::$sendEvents as $k => & $event)
        {
            $i++;

            if ($i % 100 === 0)
            {
                # 更新状态
                $this->updateStatus();
            }

            $rs = self::checkAckByEvent($event);

            if ($rs)
            {
                unset(self::$sendEvents[$k]);
                $success++;
            }
            elseif (false === $rs)
            {
                $fail++;
                list ($tag, $data, $retryNum) = $event;

                # 移除当前的对象
                unset(self::$sendEvents[$k]);

                if ($data)
                {
                    self::sendToFluent($tag, $data, $retryNum + 1);
                }
            }
        }

        if (IS_DEBUG && ($success || $fail))
        {
            if (time() - $this->doTime['debug.ack'] >= 3)
            {
                debug("Task#$this->taskId get ack response success $success". ($fail ? ", fail: $fail" : '') .", use time: " . (microtime(1) - $time) . "s");

                $success                   = 0;
                $fail                      = 0;
                $this->doTime['debug.ack'] = time();
            }
        }
    }

    /**
     * 检查ACK返回
     *
     *   * true  - 成功
     *   * false - 失败（超时）
     *   * 0     - 还需要再检测
     *
     * @param $event
     * @return bool|int
     */
    protected static function checkAckByEvent(& $event)
    {
        try
        {
            list($tag, $data, $retryNum, $time, $socket, $acks) = $event;

            $rs = @fread($socket, 10240);
            if ($rs)
            {
                # 如果提交多个数据提交, 会一次返回多个,类似: {"ack":"f123"}{"ack":"f456"}
                foreach (explode('}{', $rs) as $item)
                {
                    $item = json_decode('{' . trim($item, '{}') . '}', true);
                    if ($item)
                    {
                        $ack = $item['ack'];
                        if (isset($acks[$ack]))
                        {
                            unset($acks[$ack]);
                            unset($event[5][$ack]);
                        }
                    }
                }

                if (!$acks)
                {
                    # 成功
                    @fclose($socket);

                    return true;
                }
                else
                {
                    return 0;
                }
            }
            elseif (false === $rs || microtime(1) - $time > 300)
            {
                # 超时300秒认为失败
                # 关闭
                @fclose($socket);

                warn("get ack response timeout, tag: {$tag}, retryNum: {$retryNum}");

                return false;
            }
            else
            {
                return 0;
            }
        }
        catch (Exception $e)
        {
            warn($e->getMessage());

            return 0;
        }
    }

    /**
     * 获取任务数
     *
     * @return int
     */
    public static function getJobCount()
    {
        $count = 0;
        foreach (self::$jobs as $jobs)
        {
            $count += count($jobs);
        }

        return $count;
    }

    /**
     * 将数据发送到Fluent上
     *
     * 返回数字则表示投递成功了n个
     *
     * [!!] 此处返回 true 只是表示成功投递, 并不表示服务器返回了ACK确认, 系统会每隔几秒去读取一次ACK确认
     *
     * @param string $tag
     * @param array $data
     * @param int $retryNum
     * @return bool|int
     */
    protected static function sendToFluent($tag, & $data, $retryNum = 0)
    {
        # 有相同的tag序列
        if (isset(self::$sendEvents[$tag]))return false;

        $socket = @stream_socket_client(self::$outputConfig['link'], $errno, $errstr, 2, STREAM_CLIENT_CONNECT);
        if (!$socket)
        {
            warn($errstr);
            return false;
        }
        stream_set_timeout($socket, 0, 5);

        if ($retryNum > 2)
        {
            # 大于2次错误后, 将数据分割小块投递
            $limitLen = 300000;
        }
        else
        {
            $limitLen = 3000000;
        }

        # 分块发送已发送的数据
        $send  = [];
        $num   = 0;
        $count = count($data);
        $len   = 0;
        $str   = '';
        $acks  = [];

        foreach ($data as $key => $item)
        {
            $num++;
            if (2 === $retryNum)
            {
                # 检查下数据是否有问题, 有问题的直接跳过
                if (!@json_decode($item, false))
                {
                    warn("ignore error json string: $item");
                    $item = false;
                }
            }

            if ($item)
            {
                $len += strlen($item);
                $str .= $item . ',';

                if (!is_string($item))
                {
                    echo "error data type: ";
                    var_dump($item);
                }
            }

            if ($len > $limitLen || $count === $num)
            {
                # 每 3M 分开一次推送, 避免一次发送的数据包太大
                $ack    = uniqid('f');
                $buffer = '["' . $tag . '",[' . substr($str, 0, -1) . '],{"chunk":"' . $ack . '"}]' . "\r\n";
                $len    = strlen($buffer);
                $rs     = @fwrite($socket, $buffer, $len);

                if ($rs == $len)
                {
                    # 重置后继续
                    $str        = '';
                    $len        = 0;
                    $acks[$ack] = 1;
                    if ($item)
                    {
                        $send[$key] = $item;
                    }

                    if (count($acks[$ack]) >= 10)
                    {
                        # 超过10个则返回
                        self::$sendEvents[$tag] = [$tag, $send, $retryNum, microtime(1), $socket, $acks];

                        debug("send $tag fluent data $num/". count($data));

                        # 截取数据
                        $data = array_slice($data, $num, null, true);

                        # 发送剩下的数据
                        return $num;
                    }
                }
                else
                {
                    # 如果推送失败
                    if ($send)
                    {
                        # 将已经发送成功的放在处理对象里
                        self::$sendEvents[$tag] = [$tag, $send, $retryNum, microtime(1), $socket, $acks];

                        # 截取未成功的数据
                        if ($num > 0)
                        {
                            $data = array_slice($data, $num, null, true);
                        }

                        # 返回成功数
                        return $num;
                    }
                    else
                    {
                        fclose($socket);
                        return false;
                    }
                }
            }
        }

        self::$sendEvents[$tag] = [$tag, $data, $retryNum, microtime(1), $socket, $acks];

        # 表示全部发送完毕
        return true;
    }


    /**
     * 获取任务延时处理的时间规则
     *
     * @param $set
     * @return int
     */
    protected static function getDelayTime(DataJob $job)
    {
        if (true === $job->timeOpType)return 60;

        switch ($job->timeOpType)
        {
            case 'M':      // 分钟
            case 'i':      // 分钟
                if ($job->timeOpLimit < 10)
                {
                    return 60;
                }
                else
                {
                    return 600;
                }

            case 's':      // 秒
            case '-':      // 不分组
                return 60;

            default:
                # 其它的保存间隔为10分钟
                return 600;
        }
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
            $ssdb = null;
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