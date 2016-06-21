<?php
/**
 * 任务进程对象
 */
class TaskProcess
{
    protected $id;

    /**
     * 任务进程ID
     *
     * @var int
     */
    protected $taskId;

    /**
     * 子进程ID
     *
     * @var int
     */
    public $pid;

    /**
     * 导出的列表数据
     *
     * @var array
     */
    protected $list = [];

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

    protected $dumpFile;

    public static $sendEvents = [];

    public function __construct($taskId, $id)
    {
        $this->id     = $id;
        $this->taskId = $taskId;

        $hash           = substr(md5(EtServer::$configFile), 16, 8);
        $this->dumpFile = EtServer::$config['server']['dump_path'] .'easy-total-task-process-dump-'. $hash. '-'. $taskId .'-'. $id .'.txt';

        # 读取子进程dump出的数据
        if (is_file($this->dumpFile))
        {
            $this->loadDumpData();
        }
    }

    /**
     * 启动执行
     *
     * @return bool
     */
    public function start(TaskData $taskData)
    {
        $this->process = new swoole_process(function(swoole_process $process) use ($taskData)
        {
            # 子进程里清理下无用的数据释放内存
            TaskWorker::$jobs = [];
            taskData::clean($taskData);
            unset($taskData);

            # 子进程中注册事件监听
            declare(ticks = 1);
            $sigHandler = function($signo)
            {
                swoole_process::daemon();
                $this->dumpData();
                $this->process->freeQueue();

                exit;
            };
            pcntl_signal(SIGTERM, $sigHandler);
            pcntl_signal(SIGHUP,  $sigHandler);
            pcntl_signal(SIGINT,  $sigHandler);


            $this->isSub = true;
            $this->pid   = $process->pid;

            global $argv;
            EtServer::setProcessName("php ". implode(' ', $argv) ." [task sub process]");

            # 执行直到完成
            $this->run();

            # 蜕变成守护进程
            swoole_process::daemon();

            # 不知道为什么, 突然想 sleep 一下
            usleep(10000);

            # 退出, 不用执行 shutdown_function
            exit(1);
        });

        # 启用列队
        $this->process->useQueue(crc32("{$this->taskId}_{$this->pid}"), 1);

        $this->startTime = microtime(1);
        $this->pid       = $this->process->start();

        if ($this->pid)
        {
            debug("fork a new sub process pid is {$this->pid}");

            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * 推送数据
     *
     * @param $data
     * @return bool
     */
    public function push($data)
    {
        return $this->process->push($data);
    }

    /**
     * 执行推送数据操作
     */
    protected function run()
    {
        # 线程中处理数据
        while (true)
        {
            # 加载数据
            $this->import();

            # 导出数据
            $this->output();

            # 没任何需要处理的数据
            if (!self::$sendEvents && !$this->list)
            {
                sleep(1);
            }
        }
    }

    /**
     * 导入数据
     *
     * @param bool $all
     */
    protected function import($all = false)
    {
        # 列队太多
        if (!$all && count($this->list) > 1000)return;

        # 查看任务信息
        $stat = $this->process->statQueue();

        if ($stat['queue_num'] > 0)
        {
            # 1次最多读取 10000 条信息
            if ($all)
            {
                $num = $stat['queue_num'];
            }
            else
            {
                $num = min(10000, $stat['queue_num']);
            }

            for ($i = 0; $i < $num; $i++)
            {
                $rs = $this->process->pop(65536);
                if (false !== $rs)
                {
                    list($tag, $data) = explode(',', $rs, 2);
                    $this->list[$tag][] = $data;
                }
            }
        }

        $this->updateStatus();
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
        if ($this->list)foreach ($this->list as $tag => $value)
        {
            file_put_contents($this->dumpFile, $tag.','.serialize($value) ."\r\n", FILE_APPEND);
        }

        $this->list = [];

        # 从队列里读取所有数据
        $this->import(true);

        # 如果还有则再导出一次
        if ($this->list)foreach ($this->list as $tag => $value)
        {
            file_put_contents($this->dumpFile, $tag.','.serialize($value) ."\r\n", FILE_APPEND);
        }
    }

    /**
     * 加载数据(主进程中执行)
     */
    protected function loadDumpData()
    {
        if (!is_file($this->dumpFile))return;

        foreach (explode("\r\n", file_get_contents($this->dumpFile)) as $item)
        {
            if (!$item)continue;

            list($tag, $item) = explode(',', $item, 2);

            $tmp  = @unserialize($item);

            if ($tmp)
            {
                if (isset($this->list[$tag]))
                {
                    $this->list[$tag] = array_merge($this->list[$tag], $tmp);
                }
                else
                {
                    $this->list[$tag] = $tmp;
                }
            }
        }

        unlink($this->dumpFile);
    }

    /**
     * 将数据发送到 Fluent
     */
    protected function output()
    {
        try
        {
            if (self::$sendEvents)
            {
                $this->checkAck();
            }

            foreach ($this->list as $tag => $item)
            {
                # 发送数据
                if (count(self::$sendEvents) > 100)
                {
                    # 超过100个队列没处理
                    break;
                }

                if (self::sendToFluent($tag, $item))
                {
                    unset($this->list[$tag]);
                }
                else
                {
                    warn("push data {$tag} fail. fluent server: " . TaskData::$outputConfig['type'] . ': ' . TaskData::$outputConfig['link']);
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
        $i       = 0;
        $time    = microtime(1);
        $success = 0;
        $fail    = 0;
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

        if (IS_DEBUG && ($success || $fail))
        {
            debug("taskId: $this->taskId, no: $this->id get ack response success $success, fail: $fail, use time: " . (microtime(1) - $time) . "s");
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
        $socket = @stream_socket_client(TaskData::$outputConfig['link'], $errno, $errstr, 1, STREAM_CLIENT_CONNECT);
        if (!$socket)
        {
            warn($errstr);
            return false;
        }
        stream_set_timeout($socket, 0, 5);

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

        self::$sendEvents[] = [$data, $tag, $retryNum, microtime(1), $socket, $acks];

        return true;
    }
}
