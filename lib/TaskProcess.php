<?php
/**
 * 任务进程对象
 */
class TaskProcess
{
    /**
     * 子进程ID
     *
     * @var int
     */
    public $pid;

    /**
     * 子进程对象
     *
     * @var swoole_process
     */
    public $process;

    /**
     * 任务进程ID
     *
     * @var int
     */
    protected $taskId;

    /**
     * 进程序号
     *
     * @var int
     */
    protected $workerId;

    /**
     * 任务共享数据对象
     *
     * @var swoole_table
     */
    protected $jobsTable;

    /**
     * 分块数据
     *
     * @var array
     */
    protected $jobsTableBlockData = [];

    /**
     * 任务数据
     *
     * @var array
     */
    protected $jobs = [];

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

    /**
     * 启动时间
     *
     * @var float
     */
    protected $startTime;


    /**
     * 当前进程是否子进程
     *
     * @var bool
     */
    protected $isSub = false;

    /**
     * 数据存储驱动对象
     *
     * @var DataDriver
     */
    protected $driver;

    protected $doTime = [];

    /**
     * 程序退出时导出数据的文件路径
     *
     * @var string
     */
    public static $dumpFile;

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

    public function __construct($taskId)
    {
        $this->taskId    = $taskId;
        $this->workerId  = $taskId + EtServer::$config['conf']['worker_num'];
        $this->driver    = new DataDriver(self::$dataConfig);
        $this->jobsTable = EtServer::$jobsTable[$this->taskId];

        # 读取子进程dump出的数据
        $this->loadDumpData();
    }

    /**
     * 启动执行
     *
     * @return bool
     */
    public function start()
    {
        $this->process = new swoole_process(function(swoole_process $process)
        {
            # 子进程里清理下无用的数据释放内存
            TaskWorker::$jobs = [];

            # 子进程中注册事件监听
            declare(ticks = 1);
            $sigHandler = function($signo)
            {
                $this->dumpData();
                $this->process->freeQueue();
                swoole_process::daemon();

                exit;
            };
            pcntl_signal(SIGTERM, $sigHandler);
            pcntl_signal(SIGHUP,  $sigHandler);
            pcntl_signal(SIGINT,  $sigHandler);

            $this->process   = $process;
            $this->isSub     = true;
            $this->pid       = $process->pid;

            global $argv;
            EtServer::setProcessName("php ". implode(' ', $argv) ." [task sub process]");

            $this->run();

            # 蜕变成守护进程
            swoole_process::daemon();

            # 不知道为什么, 突然想 sleep 一下
            usleep(10000);

            # 退出, 不用执行 shutdown_function
            exit(1);
        });

        # 启用队列模式
        # $this->process->useQueue($this->taskId);

        $this->startTime = microtime(1);
        $this->pid       = $this->process->start();

        if ($this->pid)
        {
            debug("Task#$this->taskId fork a new sub process pid is {$this->pid}");

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
     * 返回列队数
     *
     * @return int
     */
    public function queueCount()
    {
        return count($this->jobsTable);
//        $stat = $this->process->statQueue();
//        return $stat['queue_num'];
    }

    /**
     * 执行推送数据操作
     */
    protected function run()
    {
        $this->doTime['clean'] = time();
        $idStr = str_pad('#'.$this->taskId, 4, ' ', STR_PAD_LEFT);
        $count = 0;

        while (true)
        {
            # 加载数据
            $count += $this->import();

            # 没有任何需要处理的信息
            if (!$count && !self::$sendEvents && !$this->list && !$this->jobs)
            {
                sleep(1);
                continue;
            }

            # 如果导入的数据比较少
            if ($count < 2000 && microtime(1) - $this->doTime['import'] < 1)
            {
                # 继续导入
                usleep(100);
                continue;
            }

            $this->doTime['import'] = microtime(1);

            if ($count > 0 && $jobCount = count($this->jobs))
            {
                debug("Task$idStr process import $count job(s), jobs count is: " . $jobCount);

                $this->doTime['debug.import'] = microtime(1);
            }

            # 任务数据处理
            $this->export();

            # 导出数据
            $this->output();

            # 更新内存占用
            if (!isset($this->doTime['updateMemory']) || time() - $this->doTime['updateMemory'] >= 60)
            {
                $redis     = self::getRedis();
                $memoryUse = memory_get_usage(true);
                if ($redis)
                {
                    /**
                     * @var Redis $redis
                     */
                    $redis->hSet('server.memory', TaskWorker::$serverName .'_'. $this->workerId .'_'. $this->pid, serialize([$memoryUse, time(), TaskWorker::$serverName, $this->workerId]));
                }
                $this->doTime['updateMemory'] = time();

                # 输出任务信息
                info("Task$idStr process total jobs: ". count($this->jobs) .", cache: ". count($this->jobsCache) .", fluent event: ". count(self::$sendEvents) .", queue: ". $this->queueCount() .", memory: ". number_format($memoryUse/1024/1024, 2) ."MB.");
            }

            # 清理数据
            if ($this->jobsTableBlockData && time() - $this->doTime['clean'] > 10)
            {
                foreach ($this->jobsTableBlockData as $key => $item)
                {
                    if (!$this->jobsTable->exist($key))
                    {
                        # 清理数据
                        unset($this->jobsTable[$key]);

                        warn("Task$idStr process memory table key#$key not found.");
                    }
                }
                $this->doTime['clean'] = time();
            }
        }
    }

    /**
     * 导入数据
     *
     * 返回导入数据量
     *
     * @return int
     */
    protected function import()
    {
        /*
        $count = 0;

        if ($queueCount = $this->queueCount())
        {
            $max = 10000 - count($this->jobs);
            if ($max < 0)
            {
                if (IS_DEBUG)
                {
                    if (time() - $this->doTime['debug.import'] >= 3)
                    {
                        debug("Task#$this->taskId process jobs is too much, count is " . count($this->jobs));
                        $this->doTime['debug.import'] = time();
                    }
                }
                return 0;
            }

            # 最多读取当前列队中的数量
            if ($max > $queueCount)
            {
                $max = $queueCount;
            }

            $buffer     = '';
            $openBuffer = false;

            while (true)
            {
                if (!$openBuffer && $count >= $max)
                {
                    break;
                }

                $str = $this->process->pop(65535);

                if ($str === 'end')
                {
                    $openBuffer = false;
                    $str        = $buffer;
                    $buffer     = '';
                }
                elseif ($str === 'begin')
                {
                    $buffer     = '';
                    $openBuffer = true;
                    continue;
                }
                elseif (substr($str, 0, 2) === '><')
                {
                    $str = substr($str, 2);
                    if ($openBuffer)
                    {
                        $openBuffer = false;
                        $buffer     = '';
                    }
                }
                elseif ($openBuffer)
                {
                    $buffer .= $str;
                    continue;
                }

                $job = @unserialize($str);

                if ($job)
                {
                    $count++;
                    $this->pushJob($job);
                }
                elseif (time() - $this->doTime['warn.data'] > 30)
                {
                    warn("Task#$this->taskId process unserialize data fail, str: $str");
                    $this->doTime['warn.data'] = time();
                }
            }
        }
        */

        $count = 0;
        $max   = 10000 - count($this->jobs);
        $data  = [];

        if ($max <= 0)return 0;

        $blockKeys = [];
        foreach ($this->jobsTable as $key => $item)
        {
            if ($key !== $item['key'])
            {
                warn("error data, key is $key, value key is {$item['key']}");
            }

            # 由于在 swoole_table foreach 时进行 del($key) 操作会出现数据错位的bug, 所以先把数据读取后再处理
            if ($item['index'] > 0)
            {
                list($k) = explode('_', $key);
                $this->jobsTableBlockData[$k][$item['index']] = $item;
                $blockKeys[] = $key;
                continue;
            }

            $count++;
            $data[$key] = $item;

            if ($count >= $max)
            {
                break;
            }
        }

        # 移除分块的数据
        foreach ($blockKeys as $key)
        {
            $this->jobsTable->del($key);
        }

        foreach ($data as $key => $item)
        {
            $str     = $item['value'];
            $delKeys = [];

            if ($item['length'] > 1)
            {
                # 多个分组数据
                for($i = 1; $i < $item['length']; $i++)
                {
                    if (isset($this->jobsTableBlockData[$key][$i]))
                    {
                        $rs = $this->jobsTableBlockData[$key][$i];
                    }
                    else
                    {
                        $rs        = $this->jobsTable->get("{$key}_{$i}");
                        $delKeys[] = "{$key}_{$i}";

                        if ($key !== $item['key'])
                        {
                            warn("error sub data, key is {$key}_{$i}, value key is {$rs['key']}");
                        }
                    }

                    if ($rs)
                    {
                        $str .= $rs['value'];
                    }
                    elseif (microtime(1) - $this->doTime['warn.get_data_fail'] > 1)
                    {
                        #读取失败
                        warn("Task#$this->taskId process get swoole_table fail, key: {$key}_{$i}");
                        $this->doTime['warn.get_data_fail'] = microtime(1);
                    }
                }
            }

            $count++;

            $job = @unserialize($str);
            if ($job)
            {
                $this->pushJob($job);
            }
            elseif (microtime(1) - $this->doTime['warn.data_fail'] >= 1)
            {
                warn("Task#$this->taskId process unserialize data fail, string: $str");
                $this->doTime['warn.data_fail'] = microtime(1);
            }

            # 移除数据
            if ($delKeys)foreach ($delKeys as $key)
            {
                $this->jobsTable->del($key);
            }

            # 移除已经预加载的数据
            if (isset($this->jobsTableBlockData[$key]))
            {
                unset($this->jobsTableBlockData[$key]);
            }

            $this->jobsTable->del($key);
        }

        $this->updateStatus();

        return $count;
    }


    /**
     * 添加一个新的任务数据
     *
     * @param DataJob $job
     */
    protected function pushJob(DataJob $job)
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
        elseif ($job->total->all)
        {
            # 充数据中加载的
            $this->jobs[$job->uniqueId] = $job;
        }
        else
        {
            # 加载旧数据
            $oldJob = $this->driver->getTotal($job->uniqueId);
            if ($oldJob)
            {
                # 合并统计
                $job->mergeTotal($oldJob);
            }

            # 设置对象
            $this->jobs[$job->uniqueId] = $job;
        }
    }

    /**
     * 导出数据
     *
     * @param int $taskWorkerId
     */
    protected function export()
    {
        if ($this->jobs)
        {
            if ($count = count($this->jobsCache) > 50000)
            {
                # 如果数据太多则清理下
                $this->jobsCache = array_slice($this->jobsCache, -5000, null, true);

                debug("Task#$this->taskId process clean jobs cache, count: $count");
            }

            if (IS_DEBUG)
            {
                static $success = 0;
                static $fail    = 0;
            }
            else
            {
                $success = 0;
                $fail    = 0;
            }

            foreach ($this->jobs as $job)
            {
                /**
                 * @var DataJob $job
                 */
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

                    $success++;
                }
                else
                {
                    $fail++;
                }

                if ($success % 100 === 0)
                {
                    $this->updateStatus();
                }
            }

            if (IS_DEBUG)
            {
                if ($success || $fail)
                {
                    if (time() - $this->doTime['debug.export'] >= 3)
                    {
                        $success = 0;
                        $fail    = 0;
                        $this->doTime['debug.export'] = time();

                        debug("Task#$this->taskId process jobs count: " . count($this->jobs) . ", success: $success, fail: $fail.");
                    }
                }
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
                $this->list[$tag][] = json_encode([$job->time, $data], JSON_UNESCAPED_UNICODE);
            }
        }

        return true;
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
            file_put_contents(self::$dumpFile, $tag .','. serialize($value) ."\r\n", FILE_APPEND);
        }

        if ($this->jobs)foreach ($this->jobs as $job)
        {
            /**
             * @var DataJob $job
             */
            file_put_contents(self::$dumpFile, 'job,'. serialize($job) ."\r\n", FILE_APPEND);
        }

        # 将任务中的数据导出
        /*
        $count      = $this->queueCount();
        $buffer     = '';
        $openBuffer = false;
        if ($count)for($i = 1; $i <= $count; $i++)
        {
            $str = $this->process->pop(65536);

            if ($str === 'end')
            {
                $openBuffer = false;
                $str        = $buffer;
                $buffer     = '';
            }
            elseif ($str === 'begin')
            {
                $buffer     = '';
                $openBuffer = true;
                continue;
            }
            elseif (substr($str, 0, 2) === '><')
            {
                $str = substr($str, 2);
            }
            elseif ($openBuffer)
            {
                $buffer .= $str;
                continue;
            }

            $job = @unserialize($str);
            if ($job)
            {
                file_put_contents(self::$dumpFile, 'job,'.serialize($job) ."\r\n", FILE_APPEND);
            }
        }
        */

        # 先读取到内存中
        $data = [];
        foreach ($this->jobsTable as $key => $item)
        {
            if ($item['index'] > 0)
            {
                list($k) = explode('_', $key);
                $this->jobsTableBlockData[$k][$item['index']] = $item;
            }
            else
            {
                $data[$key] = $item;
            }
        }

        # 写入到文件
        foreach ($data as $key => $item)
        {
            $str = $item['value'];

            if ($item['length'] > 1)
            {
                # 多个分组数据
                for ($i = 1; $i < $item['length']; $i++)
                {
                    if (isset($this->jobsTableBlockData[$key][$i]))
                    {
                        $str .= $this->jobsTableBlockData[$key][$i];
                    }
                    else
                    {
                        continue;
                    }
                }
            }

            $job = @unserialize($str);
            if ($job)
            {
                file_put_contents(self::$dumpFile, 'job,'.serialize($job) ."\r\n", FILE_APPEND);
            }
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

            list($type, $item) = explode(',', $item, 2);

            $tmp  = @unserialize($item);

            if ($tmp)
            {
                if ($type === 'job')
                {
                    /**
                     * @var DataJob $tmp
                     */
                    if (isset($this->jobs[$tmp->uniqueId]))
                    {
                        # 合并数据
                        $this->jobs[$tmp->uniqueId]->merge($tmp);
                    }
                    else
                    {
                        $this->jobs[$tmp->uniqueId] = $tmp;
                    }
                }
                else
                {
                    $this->list[$type] = $tmp;
                }
            }
        }

        unlink(self::$dumpFile);
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
                    warn("Task#$this->taskId process push data {$tag} fail. fluent server: " . self::$outputConfig['type'] . ': ' . self::$outputConfig['link']);
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

        if (IS_DEBUG)
        {
            static $success = 0;
            static $fail    = 0;
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
            if (time() - $this->doTime['debug.ack'] >= 3)
            {
                $success = 0;
                $fail    = 0;
                $this->doTime['debug.ack'] = time();

                debug("Task#$this->taskId get ack response success $success, fail: $fail, use time: " . (microtime(1) - $time) . "s");
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

            $rs = @fread($socket, 10240);
            if ($rs)
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
        $socket = @stream_socket_client(self::$outputConfig['link'], $errno, $errstr, 3, STREAM_CLIENT_CONNECT);
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
                $len    = strlen($buffer);
                $rs     = @fwrite($socket, $buffer, $len);
                if ($rs == $len)
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
            $len    = strlen($buffer);
            $rs     = @fwrite($socket, $buffer, $len);
            if ($rs == $len)
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
