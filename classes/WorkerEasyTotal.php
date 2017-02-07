<?php
class WorkerEasyTotal extends MyQEE\Server\WorkerTCP
{
    /**
     * 查询任务列表
     *
     * @var array
     */
    public $queries = [];

    /**
     * 序列设置
     *
     * @var array
     */
    public $series = [];

    /**
     * 保存app相关数据, 每分钟自动清理
     *
     * @var array
     */
    public $jobAppList = [];

    /**
     * 按表分组的序列列表
     *
     * @var array
     */
    public $jobsGroupByTable = [];

    /**
     * 集群服务器列表
     *
     * @var array
     */
    public $clusterServers = [];

    /**
     * ssdb 对象
     *
     * @var redis
     */
    public $redis;

    /**
     * 是否采用的ssdb
     *
     * @see http://ssdb.io/
     * @var bool
     */
    public $isSSDB = false;

    /**
     * SimpleSSDB 对象
     *
     * @var SimpleSSDB
     */
    public $ssdb;

    /**
     * 是否暂停接受数据
     *
     * @var bool
     */
    protected $pause = false;

    protected $autoPause = false;

    /**
     * 需要刷新的任务数据
     *
     * @var FlushData
     */
    protected $flushData;

    /**
     * 当redis,ssdb等不可写入时程序又需要终止时临时导出的文件路径（确保数据安全）
     *
     * @var string
     */
    protected $dumpFile = '';

    /**
     * 需要延时关闭的
     *
     * @var array
     */
    protected $delayCloseFd = [];

    /**
     * Fluent对象
     *
     * @var FluentInForward
     */
    protected $fluentInForward;

    /**
     * 是否完成了初始化
     *
     * @var bool
     */
    private $isInit = false;

    protected static $packKey;
    
    /**
     * 当前时间, 会一直更新
     *
     * @var int
     */
    public static $timed;

    public function __construct($server)
    {
        parent::__construct($server);

        FlushData::$workerId   = $this->id;
        $this->dumpFile        = EtServer::$config['server']['dump_path'] .'total-dump-'. substr(md5(EtServer::$configFile), 16, 8) . '-'. $this->id .'.txt';
        $this->flushData       = new FlushData();
        # 包数据的key
        self::$packKey         = chr(146) . chr(206);
        $this->fluentInForward = new FluentInForward($server);
    }

    /**
     * 初始化后会调用
     */
    public function onStart()
    {
        if ($this->isInit)return true;

        # 设置Fluent的相关回调
        $this->fluentInForward->on('checkTag', [$this, 'onCheckTag']);
        $this->fluentInForward->on('each',     [$this, 'onEach']);
        $this->fluentInForward->on('ack',      [$this, 'onAck']);

        if (!$this->reConnectRedis())
        {
            # 如果没有连上, 则每秒重试一次
            $id = null;
            $id = swoole_timer_tick(1000, function() use (& $id)
            {
                if ($this->reConnectRedis())
                {
                    swoole_timer_clear($id);
                }
            });
            unset($id);
        }

        # 标记成已经初始化过
        $this->isInit = true;

        # 把当前服务器添加到 clusterServers 里
        $this->clusterServers[self::$serverName] = $this->getCurrentServerData() + ['isSelf' => true];

        # 每3秒执行1次
        $this->timeTick(3000, function()
        {
            # 更新时间戳
            self::$timed = time();

            # 检查redis
            if (!$this->redis)
            {
                # 重连
                $this->reConnectRedis();
            }

            $this->checkRedis();
        });

        # 读取未处理完的数据
        $this->loadDumpData();

        if ($this->redis)
        {
            # 注册服务器
            $this->updateServerStatus();

            # 加载设置
            $this->reloadSetting();
        }
        else
        {
            $id = null;
            $id = swoole_timer_tick(3000, function() use (& $id)
            {
                if ($this->redis)
                {
                    $this->updateServerStatus();
                    $this->reloadSetting();

                    # 退出循环
                    swoole_timer_clear($id);
                    unset($id);
                }
            });
            unset($id);
        }

        # 定时推送
        $this->timeTick(intval(EtServer::$config['server']['merge_time_ms'] ?: 5000), function()
        {
            try
            {
                $this->flush();
            }
            catch (Exception $e)
            {
                # 避免正好在处理数据时redis连接失败抛错导致程序终止, 系统会自动重连
                $this->checkRedis();
            }
        });

        # 每分钟处理1次
        $this->timeTick(60000, function()
        {
            self::$timed = time();

            # 清空AppList列表
            $this->jobAppList = [];

            # 清理延迟关闭的连接
            if ($this->delayCloseFd)
            {
                foreach ($this->delayCloseFd as $fd)
                {
                    try
                    {
                        $this->fluentInForward->closeConnect($fd);
                    }
                    catch (Exception $e)
                    {

                    }
                }

                $this->delayCloseFd = [];
            }

            # 更新任务
            $this->updateJob();

            if ($this->redis)
            {
                # 更新监控内存
                $this->redis->hSet('server.memory', self::$serverName .'_'. $this->id, serialize([memory_get_usage(true), self::$timed, self::$serverName, $this->id]));
            }
        });

        # 每10分钟处理1次
        $this->timeTick(1000 * 600, function()
        {
            # 更新配置
            $this->reloadSetting();
        });

        # 进程定时重启, 避免数据没清理占用较大内存的情况
        swoole_timer_tick(mt_rand(1000 * 3600 * 2, 1000 * 3600 * 3), function()
        {
            $this->info('now restart worker#'. $this->id);
            $this->flush();
            $this->shutdown();
            exit;
        });


        # 只有需要第一个进程处理
        if ($this->id == 0)
        {
            swoole_timer_tick(1000 * 30, function()
            {
                # 更新服务器信息
                $this->updateServerStatus();
            });

            # 输出到控制台信息
            foreach ($this->queries as $key => $query)
            {
                $this->info("fork sql({$key}): {$query['sql']}");
            }
        }

        return true;
    }


    /**
     * 接受到数据
     *
     * @param \Swoole\Server $server
     * @param $fd
     * @param $fromId
     * @param $data
     * @return bool
     */
    public function onReceive($server, $fd, $fromId, $data)
    {
        if (!$this->redis || $this->pause)
        {
            # 没有连接上redis, 或者是还存在没处理完的数据
            # 关闭连接, 不接受任何数据
            $this->fluentInForward->clearBuffer($fd);

            # 如果立即关闭的话, 推送数据的程序会立即重新连接上重新推送数据
            # 所以先放到延迟关闭的数组里, 系统会每1分钟处理1次关闭连接
            $this->delayCloseFd[$fd] = $fd;

            return false;
        }

        return $this->fluentInForward->onReceive($server, $fd, $fromId, $data);
    }

    /**
     * 检查tag是否要处理
     *
     * @param $tag
     * @param $extra
     * @return bool
     */
    public function onCheckTag(& $tag, & $extra)
    {
        # example: xd.game.hsqj.consume : $app = hsqj, $table = consume
        # example: consume: $app = '', $table = consume
        list($app, $table) = array_splice(explode('.', $tag), -2);
        if (!$table)
        {
            $table = $app;
            $app   = 'default';
        }
        $extra['table'] = $table;
        $extra['app']   = $app;

        if (isset($this->jobsGroupByTable[$table]) && $this->jobsGroupByTable[$table])
        {
            # 没有相应tag的任务, 直接跳过
            $haveTask = true;
        }
        else
        {
            $haveTask = false;
        }

        return $haveTask;
    }

    /**
     * 遍历循环处理数据
     *
     * @param $tag
     * @param $records
     * @param $extra
     * @return bool
     */
    public function onEach($tag, $records, & $extra)
    {
        # 这边的 job 是根据 sql 生成出的数据序列处理任务, 通常情况下是 1个 sql 对应1个序列任务
        # 但2个相同 group by, from 和 where 条件的 sql 则共用一个序列任务, 例如:
        # select count(*) from test group time 1h where id in (1,2,3)
        # 和
        # select sum(value) from test group time 1h where id in (1,2,3) save as test_sum
        # 占用相同序列任务
        #
        # 这样设计的目的是共享相同序列减少数据运算,储存开销

        # $extra 是在tag回调里解析出来的
        $app   = $extra['app'];
        $table = $extra['table'];
        $jobs  = [];

        foreach ($this->jobsGroupByTable[$table] as $key => $job)
        {
            if (!$job['allApp'] && ($job['for'] && !$job['for'][$app]))
            {
                # 这个任务是为某个APP定制的
                continue;
            }

            $jobs[$key] = $job;
        }

        if ($jobs)
        {
            try
            {
                $count = $extra['count'] = count($records);

                if (IS_DEBUG)
                {
                    $this->debug("worker: $this->id, tag: $tag, records count: " . $count);
                }

                # 统计用的当前时间的key
                $dayKey = date('Ymd,H:i');

                # 记录APP统计的起始时间
                $appBeginTime = microtime(1);

                foreach ($jobs as $key => $job)
                {
                    if (!isset($this->jobAppList[$key][$app]))
                    {
                        # 增加app列表映射, 用于后期数据管理
                        $this->flushData->apps[$key][$app] = $this->jobAppList[$key][$app] = self::$timed;
                    }

                    # 记录当前任务的起始时间
                    $beginTime = microtime(1);
                    foreach ($records as $record)
                    {
                        # 处理数据
                        $time = isset($record[1]['time']) && $record[1]['time'] > 0 ? $record[1]['time'] : $record[0];

                        $this->doJob($job, $app, $time, $record[1]);
                    }

                    # 序列的统计数据
                    $this->flushData->counter[$key][$dayKey]['total'] += $count;
                    $this->flushData->counter[$key][$dayKey]['time']  += 1000000 * (microtime(1) - $beginTime);
                }

                # APP的统计数据
                $this->flushData->counterApp[$app][$dayKey]['total'] += $count;
                $this->flushData->counterApp[$app][$dayKey]['time']  += 1000000 * (microtime(1) - $appBeginTime);
            }
            catch (Exception $e)
            {
                # 执行中报错, 可能是redis出问题了
                $this->warn($e->getMessage());

                # 重置临时数据
                $this->flushData->restore();

                # 检查连接
                $this->checkRedis();

                return false;
            }
        }

        return true;
    }

    /**
     * 当返回ACK确认后回调
     *
     * @param $status
     * @param $extra
     */
    public function onAck($status, $extra)
    {
        if ($status)
        {
            # 发送成功

            # 标记为任务完成
            $this->flushData->commit();

            # 计数器增加
            if ($extra['count'] > 0)
            {
                EtServer::$counter->add($extra['count']);
            }
        }
        else
        {
            # 发送失败, 恢复数据
            $this->flushData->restore();
        }
    }

    public function onConnect($server, $fd, $fromId)
    {
        $this->fluentInForward->onConnect($server, $fd, $fromId);
    }

    public function onClose($server, $fd, $fromId)
    {
        $this->fluentInForward->onClose($server, $fd, $fromId);
    }

    /**
     * 暂停服务器接受数据
     */
    public function pause()
    {
        $this->pause = true;
        $this->fluentInForward->cleanAll();
    }

    /**
     * 取消暂停
     */
    public function stopPause()
    {
        if ($this->delayCloseFd)
        {
            # 有延迟的需要关闭的连接, 先把这些连接全部关闭了
            foreach ($this->delayCloseFd as $fd)
            {
                try
                {
                    $this->fluentInForward->closeConnect($fd);
                }
                catch(Exception $e){}
            }
            $this->delayCloseFd = [];
        }

        $this->pause = false;
    }

    /**
     * 重新连接redis服务器
     *
     * @return bool
     */
    protected function reConnectRedis()
    {
        if (EtServer::$config['redis'][0])
        {
            list ($host, $port) = explode(':', EtServer::$config['redis'][0]);
        }
        else
        {
            $host = EtServer::$config['redis']['host'];
            $port = EtServer::$config['redis']['port'];
        }

        try
        {
            if (EtServer::$config['redis']['hosts'] && count(EtServer::$config['redis']['hosts']) > 1)
            {
                if (IS_DEBUG && $this->id == 0)
                {
                    $this->debug('redis hosts: '. implode(', ', EtServer::$config['redis']['hosts']));
                }

                $redis = new RedisCluster(null, EtServer::$config['redis']['hosts']);
            }
            else
            {
                $redis = new redis();

                if (false === $redis->connect($host, $port))
                {
                    throw new Exception("connect redis://$host:$port redis error");
                }
            }

            $this->redis = $redis;

            if (false === $redis->time(0))
            {
                # 大部分用redis的操作, 部分不兼容的用这个对象来处理
                $this->isSSDB = true;
                $this->ssdb   = new SimpleSSDB($host, $port);
            }

            $id = null;
            unset($id);

            return true;
        }
        catch (Exception $e)
        {
            if ($this->id == 0 && time() % 10 == 0)
            {
                $this->debug($e->getMessage());
                $this->info('redis server is not start, wait start redis://' . (EtServer::$config['redis']['hosts'] ? implode(', ', EtServer::$config['redis']['hosts']) : $host .':'. $port));
            }

            return false;
        }
    }

    /**
     * 处理数据
     *
     * @param $queryKey
     * @param $option
     * @param $app
     * @param $table
     * @param $time
     * @param $item
     */
    protected function doJob($option, $app, $time, $item)
    {
        if ($option['where'])
        {
            if (false === self::checkWhere($option['where'], $item))
            {
                # 不符合where条件
                return;
            }
        }

        $key = $option['key'];
        $fun = $option['function'];

        # 分组值
        $groupValue = [];
        if ($option['groupBy'])
        {
            foreach ($option['groupBy'] as $group)
            {
                $groupValue[] = $item[$group];
            }
        }

        if ($groupValue)
        {
            $groupValue = implode('_', $groupValue);
            if (strlen($groupValue) > 60 || preg_match('#[^a-z0-9_\-]+#i', $groupValue))
            {
                # 分组拼接后 key 太长
                # 有特殊字符
                $groupValue = 'hash-' . md5($groupValue);
            }
        }
        else
        {
            $groupValue = '';
        }

        # 多序列分组统计数据
        foreach ($option['groupTime'] as $timeOptKey => $timeOpt)
        {
            # Exp: $groupTimeKey = 1M

            if ($timeOptKey === '-')
            {
                # 不分组
                $timeKey = 0;
                $id      = $groupValue ?: (isset($item['_id']) && $item['_id'] ? $item['_id'] : md5(json_decode($item, JSON_UNESCAPED_UNICODE)));
            }
            else
            {
                # 获取时间key, Exp: 20160610123
                $timeKey = getTimeKey($time, $timeOpt[0], $timeOpt[1]);
                # 数据的ID
                $id      = $groupValue ? "{$timeKey}_{$groupValue}" : $timeKey;
            }

            # 数据的唯一key, Exp: abcde123af32,1d,hsqj,2016001,123_abc
            $uniqueId = "$key,$timeOptKey,$app,$timeKey,$groupValue";
            # 任务ID
            $taskId   = DataJob::getTaskId($uniqueId);

            # 设置到备份里
            $this->flushData->setBackup($taskId, $uniqueId);

            if (isset($this->flushData->jobs[$taskId][$uniqueId]))
            {
                $dataJob = $this->flushData->jobs[$taskId][$uniqueId];
            }
            else
            {
                $dataJob = new DataJob($uniqueId);
                $dataJob->seriesKey   = $key;
                $dataJob->dataId      = $id;
                $dataJob->timeOpLimit = $timeOpt[0];
                $dataJob->timeOpType  = $timeOpt[1];
                $dataJob->timeKey     = $timeKey;
                $dataJob->time        = $time;
                $dataJob->app         = $app;

                $this->flushData->jobs[$taskId][$uniqueId] = $dataJob;
            }

            $dataJob->setData($item, $fun, $option['allField']);
        }

        return;
    }

    /**
     * 退出程序
     */
    public function shutdown()
    {
        $this->dumpData();
    }

    /**
     * 启动时加载临时数据
     */
    protected function loadDumpData()
    {
        $count = $this->loadDumpDataFromFile($this->dumpFile);
        if ($count)
        {
            $this->info("worker($this->id) load {$count} job(s) from file {$this->dumpFile}.");
        }

        # 只需要第一个进程执行
        if ($this->id === 0)
        {
            # 如果调小过 task worker num, 需要把之前的 dump 的数据重新 load 回来
            $files = preg_replace('#\-'. $this->id .'\.txt$#', '-*.txt', $this->dumpFile);

            # 所有任务数减1则为最大任务数的序号
            $maxIndex = $this->server->setting['worker_num'] - 1;
            foreach (glob($files) as $file)
            {
                if (preg_match('#\-(\d+)\.txt$#', $file, $m))
                {
                    if ($m[1] > $maxIndex)
                    {
                        # 序号大于最大序号
                        $count = $this->loadDumpDataFromFile($file);

                        if ($count)
                        {
                            $this->info("worker($this->id) load {$count} job(s) from file {$file}.");
                        }
                    }
                }
            }
        }
    }

    /**
     * 加载数据
     *
     * @param $file
     * @return bool|int
     */
    protected function loadDumpDataFromFile($file)
    {
        if (is_file($file))
        {
            $count = 0;
            foreach (explode("\0\r\n", file_get_contents($file)) as $item)
            {
                if (!$item)continue;

                $job = @msgpack_unpack($item);

                if ($job && $job instanceof DataJob)
                {
                    $taskId   = $job->taskId();
                    $uniqueId = $job->uniqueId;

                    $this->flushData->jobs[$taskId][$uniqueId] = $job;
                    $count++;
                }
                else
                {
                    $this->warn("load data error: ". $item);
                }
            }

            unlink($file);

            return $count;
        }
        else
        {
            return false;
        }
    }

    /**
     * 在程序退出时保存数据
     */
    public function dumpData()
    {
        if ($this->flushData->jobs)
        {
            # 有数据
            foreach ($this->flushData->jobs as $item)
            {
                foreach ($item as $job)
                {
                    file_put_contents($this->dumpFile, msgpack_pack($job) . "\0\r\n", FILE_APPEND);
                }
            }
        }
    }

    /**
     * 更新相关设置
     *
     * @use $this->updateJob()
     * @return bool
     */
    public function reloadSetting()
    {
        if (!$this->redis)return false;

        # 更新集群服务器列表
        $servers = $this->redis->hGetAll('servers');
        if ($servers)
        {
            foreach ($servers as $key => $item)
            {
                $item = @json_decode($item, true);
                if ($item)
                {
                    if ($key === self::$serverName)
                    {
                        $item['isSelf'] = true;
                    }
                    else
                    {
                        $item['isSelf'] = false;
                    }

                    $servers[$key] = $item;
                }
                else
                {
                    unset($servers[$key]);
                }
            }
        }
        else
        {
            $servers = $this->getCurrentServerData() + ['isSelf' => true];
        }
        $this->clusterServers = $servers;

        # 更新序列设置
        $this->series  = array_map('unserialize', $this->redis->hGetAll('series'));

        # 更新查询任务设置
        $this->queries = array_map('unserialize', $this->redis->hGetAll('queries'));

        # 更新任务
        $this->updateJob();

        return true;
    }

    /**
     * 更新任务
     */
    protected function updateJob()
    {
        $job = [];
        self::$timed = time();

        foreach ($this->queries as $key => $opt)
        {
            if (!$opt['use'])
            {
                if ($this->id == 0)
                {
                    $this->debug("query not use, key: {$opt['key']}, table: {$opt['table']}");
                }
                continue;
            }
            elseif ($opt['deleteTime'] > 0)
            {
                # 已经标记为移除了的任务
                $seriesKey = $opt['seriesKey'];
                if ($this->series[$seriesKey])
                {
                    $k = array_search($key, $this->series[$seriesKey]['queries']);
                    if (false !== $k)
                    {
                        unset($this->series[$seriesKey]['queries'][$k]);
                        $this->series[$seriesKey]['queries'] = array_values($this->series[$seriesKey]['queries']);
                    }
                }

                continue;
            }

            # 当前序列的key
            $seriesKey = $opt['seriesKey'];

            if (!$this->series[$seriesKey])
            {
                # 被意外删除? 动态更新序列
                $this->series[$seriesKey] = WorkerAPI::createSeriesByQueryOption($opt, $this->queries);

                # 更新服务器的
                $this->redis->hSet('series', $seriesKey, serialize($this->series[$seriesKey]));
            }

            if ($this->series[$seriesKey]['start'] && $this->series[$seriesKey]['start'] - self::$timed > 60)
            {
                # 还没到还是时间
                continue;
            }

            if ($this->series[$seriesKey]['end'] && self::$timed > $this->series[$seriesKey]['end'])
            {
                # 已经过了结束时间
                continue;
            }

            $job[$opt['table']][$seriesKey] = $this->series[$seriesKey];
        }

        $this->jobsGroupByTable = $job;
    }

    /**
     * 更新服务器状态
     *
     * @return bool
     */
    protected function updateServerStatus()
    {
        if (!$this->redis)return false;

        return $this->redis->hSet('servers', self::$serverName, json_encode($this->getCurrentServerData())) ? true : false;
    }

    /**
     * 获取当前服务器集群数据
     *
     * @return array
     */
    protected function getCurrentServerData()
    {
        return [
            'stats'      => $this->server->stats(),
            'updateTime' => self::$timed,
            'api'        => 'http://'. EtServer::$config['manager']['host'] .':'. EtServer::$config['manager']['port'] .'/api/',
        ];
    }

    /**
     * 刷新数据到redis,ssdb 刷新间隔默认3秒
     *
     * @return bool
     */
    protected function flush()
    {
        try
        {
            if ($this->flushData->jobs)
            {
                $time    = microtime(1);
                $count   = $this->flushData->flush();
                $useTime = microtime(1) - $time;

                if (IS_DEBUG && ($count || $this->flushData->delayCount))
                {
                    $this->debug('Worker#' . $this->id . " flush {$count} jobs, use time: {$useTime}s" . ($this->flushData->delayCount > 0 ? ", delay jobs: {$this->flushData->delayCount}." : '.'));
                }

                $timeKey = date('H:i');
                $key     = "counter.flush.time." . date('Ymd');
                $this->flushData->counterFlush[$key][$timeKey] += 1000000 * $useTime;
            }

            # 推送管理数据
            $this->flushData->flushManagerData($this->redis);

            if ($this->flushData->delayCount > 30000)
            {
                if (!$this->pause)
                {
                    # 超过30000个任务没投递, 开启自动暂停
                    $this->autoPause = true;
                    $this->pause();

                    $this->warn('Worker#' . $this->id . " is busy. delay jobs: {$this->flushData->delayCount}, now pause accept new data.");
                }
            }
            elseif ($this->pause && $this->autoPause && $this->flushData->delayCount < 20000)
            {
                # 关闭自动暂停
                $this->autoPause  = false;
                $this->stopPause();

                $this->info('Worker#'. $this->id .' re-accept new data.');
            }
        }
        catch (Exception $e)
        {
            $this->warn($e->getMessage());

            # 如果有错误则检查下
            $this->checkRedis();
        }
    }

    /**
     * 检查redis连接, 如果ping不通则将 `$this->redis` 设置成 null
     */
    protected function checkRedis()
    {
        try
        {
            if ($this->redis && false === @$this->redis->ping(0))
            {
                throw new Exception('redis closed');
            }
        }
        catch(Exception $e)
        {
            $this->redis = null;
            $this->ssdb  = null;
        }
    }

    protected static function checkWhere($opt, $data)
    {
        if (isset($opt['$type']))
        {
            # 当前的类型: && 或 ||
            $type = $opt['$type'];

            foreach ($opt['$item'] as $item)
            {
                if (is_array($item) && isset($item['$type']))
                {
                    # 子分组条件
                    $rs = self::checkWhere($opt, $data);
                }
                else
                {
                    $rs    = false;
                    $isIn  = false;
                    $value = null;

                    if ($item['field'])
                    {
                        if (is_array($item['field']))
                        {
                            # 参数是字段数据
                            foreach ($item['field'] as $k => $v)
                            {
                                $item['arg'][$k] = $data[$v];
                            }
                        }
                        else
                        {
                            $value = $data[$item['field']];
                        }
                    }

                    if ($item['typeM'])
                    {
                        switch ($item['typeM'])
                        {
                            case '%':
                            case 'mod':
                                $value = $value % $item['mValue'];
                                break;
                            case '>>';
                                $value = $value >> $item['mValue'];
                                break;
                            case '<<';
                                $value = $value << $item['mValue'];
                                break;
                            case '-';
                                if (is_numeric($item['mValue']))
                                {
                                    $value = $value - $item['mValue'];
                                }
                                else
                                {
                                    $value = $value - $data[$item['mValue']];
                                }
                                break;
                            case '+';
                                if (is_numeric($item['mValue']))
                                {
                                    $value = $value + $item['mValue'];
                                }
                                else
                                {
                                    $value = $value + $data[$item['mValue']];
                                }
                                break;
                            case '*';
                            case 'x';
                                if (is_numeric($item['mValue']))
                                {
                                    $value = $value * $item['mValue'];
                                }
                                else
                                {
                                    $value = $value * $data[$item['mValue']];
                                }
                                break;

                            case '/';
                                if (is_numeric($item['mValue']))
                                {
                                    $value = $value / $item['mValue'];
                                }
                                else
                                {
                                    $value = $value / $data[$item['mValue']];
                                }
                                break;

                            case 'func':
                                switch ($item['fun'])
                                {
                                    case 'from_unixtime':
                                        $value = @date($item['arg'][1], $value);
                                        break;

                                    case 'unix_timestamp':
                                        $value = @strtotime($value);
                                        break;

                                    case 'in':
                                        $isIn = true;
                                        $rs = in_array($data[$item['field']], $item['arg']);
                                        break;

                                    case 'not_in':
                                        $isIn = true;
                                        $rs = !in_array($data[$item['field']], $item['arg']);
                                        break;

                                    default:
                                        $value = Func::callWhereFun($item['fun'], $item['arg']);
                                        break;
                                }
                                break;
                        }
                    }

                    if (!$isIn)
                    {
                        $rs = self::checkWhereEx($value, $item['fMode'] ? $data[$item['value']] : $item['value'], $item['type']);
                    }
                }

                if ($type === '&&')
                {
                    # 并且的条件, 返回了 false, 则不用再继续判断了
                    if ($rs === false)return false;
                }
                else
                {
                    # 或, 返回成功则不用再判断了
                    if ($rs === true)return true;
                }
            }
        }

        return true;
    }

    protected static function checkWhereEx($v1, $v2, $type)
    {
        switch ($type)
        {
            case '>';
                if ($v1 > $v2)
                {
                    return true;
                }
                break;
            case '<';
                if ($v1 < $v2)
                {
                    return true;
                }
                break;
            case '>=';
                if ($v1 >= $v2)
                {
                    return true;
                }
                break;
            case '<=';
                if ($v1 <= $v2)
                {
                    return true;
                }
                break;
            case '!=';
                if ($v1 != $v2)
                {
                    return true;
                }
                break;
            case '=';
            default :
                if ($v1 == $v2)
                {
                    return true;
                }
                break;
        }

        return false;
    }
}
