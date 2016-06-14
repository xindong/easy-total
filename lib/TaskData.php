<?php

if (false && class_exists('Thread', false))
{
    # todo 多线程模式待测试

    /**
     * 使用多线程模式的对象
     *
     * @see http://cn.php.net/manual/zh/class.threaded.php
     * Class DataThreaded
     */
    abstract class TaskDataThreaded extends Threaded
    {
        public $runTime = 0;

        /**
         * 任务进程ID
         *
         * @var int
         */
        public $taskId;

        /**
         * 是否完成
         *
         * @var bool
         */
        protected $done = false;

        const THREAD_MODE = true;

        /**
         * 更新状态
         *
         * @param int $status 1 - 成功, 2 - 运行中
         */
        public function updateStatus($status = 2)
        {
            $this->runTime = time();
            $this->done = $status == 2 ? false : true;
        }

        /**
         * 获取最后执行时间
         *
         * @return int
         */
        public function getLastRunTime()
        {
            return $this->runTime;
        }

        /**
         * 主进程在退出时通知子进程存档数据
         */
        public function dump()
        {
            # 导出数据
            $this->dumpData();

            # 杀掉线程
            $this->kill();
        }

        abstract protected function dumpData();
    }
}
else
{

    /**
     * 使用多进程模式的对象
     *
     * Class DataThreaded
     */
    abstract class TaskDataThreaded
    {
        public $runTime = 0;

        /**
         * 启动时间
         *
         * @var float
         */
        protected $startTime;

        /**
         * 是否完成
         *
         * @var bool
         */
        protected $done = false;

        /**
         * 任务进程ID
         *
         * @var int
         */
        public $taskId = 0;

        /**
         * @var swoole_process
         */
        protected $process;

        /**
         * 子进程ID
         *
         * @var int
         */
        protected $processPid;

        /**
         * 当前进程是否子进程
         *
         * @var bool
         */
        protected $isSubProcess = false;

        const THREAD_MODE = false;

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

                $this->startTime    = microtime(1);
                $this->isSubProcess = true;
                $this->processPid   = $process->pid;

                # 子进程里清理下无用的数据释放内存
                TaskWorker::$dist    = [];
                TaskWorker::$jobTime = [];
                TaskWorker::$jobs    = [];
                TaskWorker::$total   = [];

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

            $this->processPid = $this->process->start();

            if ($this->processPid)
            {
                debug("fork a new sub process pid is {$this->processPid}");

                # 此时数据已经复制到了子进程里, 可以在主进程里执行清理数据释放内存
                $this->clean();

                return true;
            }
            else
            {
                return false;
            }
        }

        abstract function clean();

        abstract function run();

        abstract protected function dumpData();

        /**
         * 是否在运行中（在主进程中调用）
         *
         * @return bool
         */
        public function isRunning()
        {
            $rs = EtServer::$taskWorkerStatus->get("sub{$this->taskId}_{$this->processPid}");

            return $rs['status'] == 1 ? false : true;
        }

        /**
         * 设置状态（在子进程里设置）
         *
         * @param int $status 1 - 成功, 2 - 运行中
         */
        public function updateStatus($status = 2)
        {
            # 更新子进程状态
            EtServer::$taskWorkerStatus->set("sub{$this->taskId}_{$this->processPid}", ['time' => time(), 'status' => $status]);

            $this->done = $status == 2 ? false : true;

            if (IS_DEBUG && $this->done)
            {
                debug("sub process (pid: {$this->processPid}) has done, use time: ". (microtime(1) - $this->startTime) .'s');
            }
        }

        /**
         * 获取最后执行时间（在主进程里调用）
         *
         * @return int
         */
        public function getLastRunTime()
        {
            $rs = EtServer::$taskWorkerStatus->get("sub{$this->taskId}_{$this->processPid}");

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
            if ($this->isSubProcess)
            {
                # 子进程里不允许调用
                return false;
            }

            # 强制杀掉进程
            swoole_process::kill($this->processPid, 9);

            # 回收资源
            swoole_process::wait(true);

            return true;
        }

        /**
         * 关闭任务, 清理数据
         */
        public function close()
        {
            EtServer::$taskWorkerStatus->del("sub{$this->taskId}_{$this->processPid}");
            swoole_process::wait(true);
            $this->clean();
        }

        /**
         * 主进程在退出时通知子进程存档数据
         */
        public function dump()
        {
            # 发送一个结束程序的信号
            swoole_process::kill($this->processPid, SIGINT);

            # 回收资源
            swoole_process::wait(true);
        }
    }
}

/**
 * task worker 中用到的处理数据对象
 *
 * 兼容多线程 Thread 的处理方式, 需要安装 pthreads 扩展 see http://php.net/manual/zh/book.pthreads.php
 *
 * 这个对象在 worker 进程中无用
 */
class TaskThreaded extends TaskDataThreaded
{
    /**
     * @var DataObject
     */
    public $dist;

    /**
     * @var DataObject
     */
    public $list;

    /**
     * @var DataObject
     */
    public $jobs;

    /**
     * @var DataObject
     */
    public $total;

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

    public static $distConfig = [];

    public static $dataLink = [];

    public static $dumpPath = '/tmp/';

    public function __construct()
    {
        $this->dist  = new DataObject();
        $this->list  = new DataObject();
        $this->jobs  = new DataObject();
        $this->total = new DataObject();
    }

    public function __destruct()
    {
        $this->clean();
    }

    function clean()
    {
        $this->dist  = null;
        $this->list  = null;
        $this->jobs  = null;
        $this->total = null;
    }

    /**
     * 执行推送数据操作
     */
    public function run()
    {
        # 线程中处理数据
        while (true)
        {
            # 更新状态
            $this->updateStatus();

            $this->doSave();

            # 处理完毕
            if (!$this->done)
            {
                sleep(1);
            }
            else
            {
                break;
            }
        }
    }

    /**
     * 保存数据
     *
     * 如果使用了多线程模式, 这个方法是在新建立的线程里运行的了
     *
     * @param int $taskWorkerId
     */
    public function doSave()
    {
        if (!$this->driver)
        {
            $this->driver = new DataDriver(self::$distConfig);
        }

        # 保存唯一数
        if (count($this->dist))
        {
            foreach ($this->dist as $uniqueId => $value)
            {
                if ($this->saveDist($uniqueId))
                {
                    # 保存唯一值
                    $this->driver->saveTotal($uniqueId, $this->total[$uniqueId]);
                }
            }

            # 更新状态
            $this->updateStatus();
        }

        if (count($this->jobs))
        {
            foreach ($this->jobs as $uniqueId => $value)
            {
                # 更新下唯一序列统计值
                if ($this->dist[$uniqueId])
                {
                    if (false === $this->saveDist($uniqueId))
                    {
                        continue;
                    }

                    # 保存统计值
                    $this->driver->saveTotal($uniqueId, $this->total[$uniqueId]);
                }

                # 导出到列表
                if ($this->exportList($uniqueId, $value))
                {
                    # 移除任务列表, 释放内存
                    unset($this->jobs[$uniqueId]);
                }
            }

            # 更新状态
            $this->updateStatus();
        }

        # 导出数据
        $this->output();

        # 是否完成
        $done = $this->dist->count() || $this->jobs->count() || $this->list->count() || self::$sendEvents ? false : true;

        # 更新状态
        $this->updateStatus($done ? 1 : 2);
    }

    /**
     * 保存唯一数据
     *
     * @param $uniqueId
     * @return bool
     */
    protected function saveDist($uniqueId)
    {
        foreach ($this->dist[$uniqueId] as $field => $v)
        {
            if ($count = $this->driver->saveDist($uniqueId, $field, $v))
            {
                # 更新统计数
                if (!isset($this->total[$uniqueId]))
                {
                    $this->total[$uniqueId] = new DataTotalItem();
                }
                $this->total[$uniqueId]->dist[$field] = $count;

                # 清理数据
                unset($this->dist[$uniqueId][$field]);
                if (!count($this->dist[$uniqueId]))
                {
                    unset($this->dist[$uniqueId]);
                }
            }
            else
            {
                return false;
            }
        }

        return true;
    }

    /**
     * 导出列表数据
     *
     * @param $id
     * @param $listData
     * @return bool
     */
    public function exportList($uniqueId, $listData)
    {
        list($id, $timeOptKey, $timeKey, $time, $app, $seriesKey, $item) = $listData;

        if (!isset(TaskThreaded::$series[$seriesKey]))
        {
            # 没有对应的序列
            $redis = self::getRedis();
            if ($redis)
            {
                TaskThreaded::$series = array_map('unserialize', $redis->hGetAll('series'));
            }
            else
            {
                return false;
            }

            if (!TaskThreaded::$series[$seriesKey])
            {
                # 重新获取后序列还不存在
                return true;
            }
        }

        $seriesOption = TaskThreaded::$series[$seriesKey];
        $queries      = $seriesOption['queries'] ?: [];

        /**
         * @var DataTotalItem $total
         */
        $total = $this->total[$uniqueId];

        # 如果有函数统计并且当前的统计数据是没有更新过的, 先更新下统计数据
        if ($seriesOption['function'] && !$this->total[$uniqueId]->lastLoadTime)
        {
            try
            {
                $oldTotal = $this->driver->getTotal($uniqueId);
                if ($oldTotal)
                {
                    self::totalDataMerge($total, $oldTotal);
                    $total->lastLoadTime = time();
                }
                else
                {
                    return false;
                }
            }
            catch (Exception $e)
            {
                warn($e->getMessage());
                return false;
            }
        }

        if (isset($queries[$timeOptKey]))
        {
            foreach ($queries[$timeOptKey] as $queryKey)
            {
                if (!isset(TaskThreaded::$queries[$queryKey]))
                {
                    if (!isset($redis))
                    {
                        $redis = self::getRedis();
                        if (false === $redis)return false;
                    }

                    TaskThreaded::$queries = array_map('unserialize', $redis->hGetAll('queries'));
                }

                $queryOption = TaskThreaded::$queries[$queryKey];
                if (!$queryOption)
                {
                    # 没有对应的查询
                    continue;
                }

                # 查询已经更改
                if ($queryOption['seriesKey'] !== $seriesKey)continue;

                # 生成数据
                $data = [
                    '_id'    => $id,
                    '_group' => $timeKey,
                ];

                if ($queryOption['allField'])
                {
                    $data += $item;
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
                                $data[$as] = $total->$type[$field];
                                break;

                            case 'first':
                            case 'last':
                                $data[$as] = $total->$type[$field][0];
                                break;

                            case 'avg':
                                # 平均数
                                $sum   = $total->sum[$field];
                                $count = $total->count['*'];
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
                            $saveAs = str_replace($tmp[2], explode(',', date($tmp[3], $time)), $tmp[0]);
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
                $tag = self::$outputConfig['prefix'] ."$app.$saveAs";

                # 记录到导出列表数据列
                if (!isset($this->list[$tag]))
                {
                    $this->list[$tag] = [];
                }

                # 加入列表
                $this->list[$tag][] = json_encode([$time, $data], JSON_UNESCAPED_UNICODE);
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
     * 恢复数据重启
     *
     * @use $this->start()
     * @param $data
     */
    public function restore($data)
    {
        $this->jobs   = $data['jobs'];
        $this->dist   = $data['dist'];
        $this->list   = $data['list'];
        $this->total  = $data['total'];

        $this->start();
    }

    protected function dumpData()
    {
        $dumpFile = self::$dumpPath . 'total-task-process-dump-' . $this->taskId . '.txt';

        $data = [
            'dist'  => $this->dist,
            'jobs'  => $this->jobs,
            'list'  => $this->list,
            'total' => $this->total,
        ];

        # 写入文件
        file_put_contents($dumpFile, serialize($data) ."\r\n", FILE_APPEND);
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
    public static function totalDataMerge(& $total, $newTotal)
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
