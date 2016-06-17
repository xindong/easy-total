<?php
/**
 * 任务进程对象
 */
class TaskProcess
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
     * 子进程ID
     *
     * @var int
     */
    public $pid;

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
     * 导出的列表数据
     *
     * @var array
     */
    protected $list = [];

    public $runTime = 0;

    /**
     * 任务数据对象
     *
     * @var swoole_table
     */
    public $jobsTable;

    /**
     * 启动时间
     *
     * @var float
     */
    protected $startTime;

    /**
     * @var swoole_process
     */
    protected $process;

    /**
     * 当前进程是否子进程
     *
     * @var bool
     */
    protected $isSub = false;

    /**
     * @var DataDriver
     */
    protected $driver;

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

    public static $sendEvents = [];

    public static $redisConfig = [];

    public static $outputConfig = [];

    public static $dataConfig = [];

    public static $dumpFile;

    public function __construct($taskId)
    {
        $this->taskId    = $taskId;
        $this->workerId  = EtServer::$config['conf']['worker_num'];
        $this->jobsTable = EtServer::$jobsTable[$taskId];
        $this->driver    = new DataDriver(self::$dataConfig);

        # 读取子进程dump出的数据
        if (is_file(self::$dumpFile))
        {
            $this->loadDumpData();
        }
    }

    public function __destruct()
    {
        $this->clean();

        EtServer::$taskWorkerStatus->del("sub{$this->taskId}_{$this->pid}");
    }

    /**
     * 启动执行
     *
     * @return bool
     */
    public function start()
    {
        # 开启一个子进程
        $this->process = new swoole_process(function(swoole_process $process)
        {
            declare(ticks = 1);

            $sigHandler = function($signo)
            {
                $this->dumpData();
                exit;
            };

            pcntl_signal(SIGTERM, $sigHandler);
            pcntl_signal(SIGHUP,  $sigHandler);
            pcntl_signal(SIGINT,  $sigHandler);

            $this->isSub = true;
            $this->pid   = $process->pid;

            global $argv;
            EtServer::setProcessName("php ". implode(' ', $argv) ." [task sub process]");

            # 子进程里清理下无用的数据释放内存
            TaskWorker::$jobs = [];

            # 执行直到完成
            $this->run();

            # 蜕变成守护进程
            swoole_process::daemon();

            # 销毁内容
            $this->clean();

            usleep(10000);
            # 退出, 不用执行 shutdown_function
            exit(1);
        });

        $this->startTime = microtime(1);
        $this->pid       = $this->process->start();

        if ($this->pid)
        {
            debug("fork a new sub process pid is {$this->pid}");

            # 此时数据已经复制到了子进程里, 可以在主进程里执行清理数据释放内存
            $this->clean();

            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * 执行推送数据操作
     */
    protected function run()
    {
        # 线程中处理数据
        $lastCleanTime = time();
        while (true)
        {
            # 更新状态
            $this->updateStatus();

            # 导入数据
            $this->importData();

            # 导出数据
            $this->exportData();

            # 导出数据
            $this->output();

            if (time() - $lastCleanTime > 60)
            {
                # 清理下数据
                $this->cleanData();

                # 更新最后清理的时间
                $lastCleanTime = time();
            }

            sleep(1);
        }
    }


    /**
     * 设置状态（在子进程里设置）
     */
    public function updateStatus()
    {
        # 更新子进程状态
        EtServer::$taskWorkerStatus->set("sub{$this->taskId}_{$this->pid}", ['time' => time()]);
    }

    /**
     * 获取最后执行时间（在主进程里调用）
     *
     * @return int
     */
    public function getActiveTime()
    {
        $rs = EtServer::$taskWorkerStatus->get("sub{$this->taskId}_{$this->pid}");

        return $rs['time'] ?: time();
    }

    /**
     * 杀死进程
     *
     * 在子进程里不能调用
     *
     * @return bool
     */
    public function kill()
    {
        if ($this->isSub)
        {
            # 子进程里不允许调用
            return false;
        }
        EtServer::$taskWorkerStatus->del("sub{$this->taskId}_{$this->pid}");

        # 强制杀掉进程
        swoole_process::kill($this->pid, 9);

        # 关闭管道
        $this->process->close();

        # 回收资源
        swoole_process::wait(false);

        return true;
    }

    /**
     * 主进程退出时通知子进程存档数据(在主进程中执行)
     */
    public function close()
    {
        # 退出子进程
        swoole_process::kill($this->pid, SIGINT);

        # 关闭管道
        $this->process->close();

        EtServer::$taskWorkerStatus->del("sub{$this->taskId}_{$this->pid}");

        # 回收资源
        swoole_process::wait(true);
    }

    /**
     * 存档数据（在子进程中执行）
     */
    protected function dumpData()
    {
        # 写入文件
        foreach ($this->jobs as $job)
        {
            file_put_contents(self::$dumpFile, 'jobs,'. serialize($job) ."\r\n", FILE_APPEND);
        }

        if ($this->list)
        {
            file_put_contents(self::$dumpFile, 'list,'. serialize($this->list) ."\r\n", FILE_APPEND);
        }
    }

    /**
     * 加载数据(主进程中执行)
     */
    protected function loadDumpData()
    {
        if (!is_file(self::$dumpFile))return;

        foreach (explode("\r\n", file_get_contents(self::$dumpFile)) as $item)
        {
            if (!$item)continue;

            $type = substr($item, 0, 4);
            $tmp  = @unserialize(substr($item, 5));

            if ($tmp)
            {
                if ($type === 'jobs')
                {
                    $this->jobs[$tmp->uniqueId] = $tmp;
                }
                elseif ($type === 'list')
                {
                    $this->list = $tmp;
                }
            }
        }

        unlink(self::$dumpFile);
    }

    /**
     * 清理数据
     */
    function clean()
    {
        $this->list  = null;
        $this->jobs  = null;
    }

    /**
     * 从共享内存块内导入数据
     */
    protected function importData()
    {
        $begin = microtime(1);
        foreach ($this->jobsTable as $key => $item)
        {
            if ($item['index'] > 0)
            {
                # 分块的片段数据
                continue;
            }

            $string  = $item['value'];
            $tmpKeys = [];
            if ($item['length'] > 1)
            {
                for ($i = 1; $i <= $item['length']; $i++)
                {
                    $tmpKey  = "$key,$i";
                    $tmp     = $this->jobsTable->get($tmpKey);

                    if ($tmp)
                    {
                        $string .= $tmp['value'];
                    }
                    else
                    {
                        usleep(3000);
                        $tmp = $this->jobsTable->get($tmpKey);
                        if ($tmp)
                        {
                            $string .= $tmp['value'];
                        }
                        else
                        {
                            warn("get swoole_table error, key: $tmpKey");
                        }
                    }
                }
            }

            /**
             * @var DataJob $job
             */
            $job = @unserialize($string);
            if (false === $job)
            {
                warn("unserialize data error, key: $key, see file /tmp/easy_total_error_unserialize_data_$key");
                file_put_contents("/tmp/easy_total_error_unserialize_data_$key", $string);
            }
            else
            {
                $uniqueId = $job->uniqueId;

                if (isset($this->jobs[$uniqueId]))
                {
                    # 在待处理的列队里存在, 直接合并数据
                    $this->jobs[$uniqueId]->merge($job);
                }
                elseif (isset($this->jobsCache[$uniqueId]))
                {
                    # 在缓存里有数据
                    $this->jobsCache[$uniqueId]->merge($job);

                    # 赋值到 jobs 列表里
                    $this->jobs[$uniqueId] = $this->jobsCache[$uniqueId];
                }
                else
                {
                    # 加载老的数据
                    $totalItem = $this->driver->getTotal($uniqueId);

                    # 读取失败则可能是存储数据的地方有问题, 不再继续读取
                    if (false === $totalItem)return;

                    # 合并统计
                    self::mergeTotal($job->total, $totalItem);

                    # 放到对象列表里进行处理
                    $this->jobs[$uniqueId] = $job;
                }

                # 设置活跃时间
                $this->jobs[$uniqueId]->achiveTime = time();

                # 标记成未导出
                $this->jobs[$uniqueId]->saved = false;
            }

            # 移除内存中数据
            $this->jobsTable->del($key);
            if ($tmpKeys)foreach($tmpKeys as $tmpKey)
            {
                $this->jobsTable->del($tmpKey);
            }

            if (count($this->jobs) > 10000 || microtime(1) - $begin > 0.5)
            {
                # 读取一定量数据
                break;
            }
        }

        return;
    }

    /**
     * 导出数据
     *
     * 如果使用了多线程模式, 这个方法是在新建立的线程里运行的了
     *
     * @param int $taskWorkerId
     */
    public function exportData()
    {
        if ($this->jobs)
        {
            if (count($this->jobsCache) > 50000)
            {
                # 如果数据太多则清理下
                $this->jobsCache = array_slice($this->jobsCache, -5000, null, true);
            }

            foreach ($this->jobs as $uniqueId => $job)
            {
                /**
                 * @var DataJob $job
                 */
                if ($job->dist)
                {
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
                    $this->jobsCache[$uniqueId] = $job;

                    # 从当前任务中移除
                    unset($this->jobs[$uniqueId]);
                }

                # 更新状态
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

        if (!isset(TaskProcess::$series[$seriesKey]))
        {
            # 没有对应的序列
            $redis = self::getRedis();
            if ($redis)
            {
                TaskProcess::$series = array_map('unserialize', $redis->hGetAll('series'));
            }
            else
            {
                return false;
            }

            if (!TaskProcess::$series[$seriesKey])
            {
                # 重新获取后序列还不存在
                return true;
            }
        }

        $seriesOption = TaskProcess::$series[$seriesKey];
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
                if (!isset(TaskProcess::$queries[$queryKey]))
                {
                    if (!isset($redis))
                    {
                        $redis = self::getRedis();
                        if (false === $redis)return false;
                    }

                    TaskProcess::$queries = array_map('unserialize', $redis->hGetAll('queries'));
                }

                $queryOption = TaskProcess::$queries[$queryKey];
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

                # 记录到导出列表数据列
                if (!isset($this->list[$tag]))
                {
                    $this->list[$tag] = [];
                }

                # 加入列表
                $this->list[$tag][] = json_encode([$job->time, $data], JSON_UNESCAPED_UNICODE);
            }
        }

        return true;
    }

    /**
     * 将数据发送到 Fluent
     *
     * @return bool
     */
    protected function output()
    {
        try
        {
            if (self::$sendEvents)
            {
                self::checkAck();
            }

            foreach ($this->list as $tag => $item)
            {
                # 发送数据
                if (self::sendToFluent($tag, $item))
                {
                    unset($this->list[$tag]);
                }
                else
                {
                    warn("push data {$tag} fail. fluent server: " . self::$outputConfig['type'] . ': ' . self::$outputConfig['link']);
                }

                $this->updateStatus();
            }

            return true;
        }
        catch (Exception $e)
        {
            warn($e->getMessage());

            if (IS_DEBUG)
            {
                echo $e->getTraceAsString();
            }

            return false;
        }
    }

    /**
     * 清理过期的任务数据
     */
    protected function cleanData()
    {
        $now = time();
        foreach ($this->jobsCache as $key => $job)
        {
            /**
             * @var DataJob $job
             */
            if ($now - $job->activeTime > 300)
            {
                # 移除对象
                unset($this->jobs[$key]);
            }
        }

        $memory = memory_get_usage(true);
        debug("task process use memory: " . number_format($memory / 1024 / 1024, 3) .'MB');

        if ($memory > 1024 * 1024 * 1024 * 2)
        {
            # 占用大量内存, 释放内存缓存中的对象
            $this->jobsCache = [];
        }

        # 更新内存使用统计
        $redis = self::getRedis();
        $redis->hSet('server.memory', TaskWorker::$serverName .'_'. $this->workerId .'_0', serialize([memory_get_usage(true), time(), TaskWorker::$serverName, $this->workerId]));
        $redis->close();
    }

    /**
     * 检查ACK返回
     */
    protected static function checkAck()
    {
        foreach (self::$sendEvents as $k => & $event)
        {
            $rs = self::checkAckByEvent($event);
            if ($rs)
            {
                unset(self::$sendEvents[$k]);
            }
            elseif (false === $rs)
            {
                list ($data, $tag, $retryNum) = $event;

                # 移除当前的对象
                unset(self::$sendEvents[$k]);

                if ($data)
                {
                    # 切分成2分重新发送
                    $len = ceil(count($data) / 2);
                    if ($len > 1)
                    {
                        self::sendToFluent($tag, array_slice($data, 0, $len), $retryNum + 1);
                        self::sendToFluent($tag, array_slice($data, $len), $retryNum + 1);
                    }
                    else
                    {
                        self::sendToFluent($tag, $data, $retryNum + 1);
                    }
                }
            }
        }

        self::$sendEvents = array_values(self::$sendEvents);
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
            list($data, $tag, $retryNum, $time, $socket, $acks) = $event;

            if ($rs = @fread($socket, 10240))
            {
                # 如果提交多个数据提交, 会一次返回多个,类似: {"ack":"f123"}{"ack":"f456"}
                foreach (explode('}{', $rs) as $item)
                {
                    $item = json_decode('{'. trim($item, '{}') .'}', true);
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

                    if (IS_DEBUG)
                    {
                        debug("get ack response : $rs, use time " . (microtime(1) - $time) . 's.');
                    }

                    return true;
                }
                else
                {
                    return 0;
                }
            }
            elseif (microtime(1) - $time > 300)
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
     * 将数据发送到Fluent上
     *
     * [!!] 此处返回 true 只是表示成功投递, 并不表示服务器返回了ACK确认, 系统会每隔几秒去读取一次ACK确认
     *
     * @param string $tag
     * @param array $data
     * @param int $retryNum
     * @return bool
     */
    protected static function sendToFluent($tag, $data, $retryNum = 0)
    {
        $socket = @stream_socket_client(self::$outputConfig['link'], $errno, $errstr, 1, STREAM_CLIENT_CONNECT);
        if (!$socket)
        {
            warn($errstr);
            return false;
        }
        stream_set_timeout($socket, 0, 3000);

        $len  = 0;
        $str  = '';
        $acks = [];

        if ($retryNum > 2)
        {
            # 大于2次错误后, 将数据分割小块投递
            $limitLen = 300000;
        }
        else
        {
            $limitLen = 3000000;
        }

        foreach ($data as $item)
        {
            if ($retryNum > 2)
            {
                # 检查下数据是否有问题, 有问题的直接跳过
                $test = @json_decode($item, false);
                if (!$test)
                {
                    warn("ignore error fluent data: $item");
                    continue;
                }
            }

            $len += strlen($item);
            $str .= $item .',';

            if ($len > $limitLen)
            {
                # 每 3M 分开一次推送, 避免一次发送的数据包太大
                $ack    = uniqid('f');
                $buffer =  '["'. $tag .'",['. substr($str, 0, -1) .'],{"chunk":"'. $ack .'"}]'."\r\n";

                if (@fwrite($socket, $buffer, strlen($buffer)))
                {
                    # 重置后继续
                    $len = 0;
                    $str = '';
                    $acks[$ack] = 1;
                }
                else
                {
                    # 如果推送失败
                    @fclose($socket);
                    return false;
                }
            }
        }

        if ($len > 0)
        {
            $ack    = uniqid('f');
            $buffer = '["'. $tag .'",['. substr($str, 0, -1) .'],{"chunk":"'. $ack .'"}]'."\r\n";

            if (@fwrite($socket, $buffer))
            {
                # 全部推送完毕
                $acks[$ack] = 1;
            }
            else
            {
                @fclose($socket);
                return false;
            }
        }

        $event = [$data, $tag, $retryNum, microtime(1), $socket, $acks];

        # 尝试去读取ACK
        $rs = self::checkAckByEvent($event);
        if (!$rs)
        {
            # 没有成功返回则放到队列里
            self::$sendEvents[] = $event;
        }

        return true;
    }

    /**
     * 合并2个统计数值
     *
     * @param $total
     * @param $newTotal
     * @return DataTotalItem
     */
    public static function mergeTotal(& $total, $newTotal)
    {
        /**
         * @var DataTotalItem $total
         * @var DataTotalItem $newTotal
         */
        # 相加的数值
        foreach ($newTotal->sum as $field => $v)
        {
            $total->sum[$field] += $v;
        }

        foreach ($newTotal->count as $field => $v)
        {
            $total->count[$field] += $v;
        }

        foreach ($newTotal->last as $field => $v)
        {
            $tmp = $total->last[$field];

            if (!$tmp || $tmp[1] < $v[1])
            {
                $total->last[$field] = $v;
            }
        }

        foreach ($newTotal->first as $field => $v)
        {
            $tmp = $total->first[$field];

            if (!$tmp || $tmp[1] > $v[1])
            {
                $total->first[$field] = $v;
            }
        }

        foreach ($newTotal->min as $field => $v)
        {
            if (isset($total->min[$field]))
            {
                $total->min[$field] = min($v, $total->min[$field]);
            }
            else
            {
                $total->min[$field] = $v;
            }
        }

        foreach ($newTotal->max as $field => $v)
        {
            if (isset($total->max[$field]))
            {
                $total->max[$field] = max($v, $total->max[$field]);
            }
            else
            {
                $total->max[$field] = $v;
            }
        }

        foreach ($newTotal->dist as $field => $v)
        {
            $total->dist[$field] = max($total->dist[$field], $v);
        }

        return $total;
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
