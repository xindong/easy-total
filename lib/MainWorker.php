<?php
class MainWorker
{
    /**
     * 当前进程ID
     *
     * @var int
     */
    public $workerId = 0;

    /**
     * @var swoole_server
     */
    public $server;

    /**
     * 是否多服务器
     *
     * @var bool
     */
    public $multipleServer = false;

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
     * 当前进程启动时间
     *
     * @var int
     */
    protected $startTime;

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
     * 记录数据的数组
     *
     * @var array
     */
    protected $buffer = [];

    /**
     * 记录数据的最后时间
     *
     * @var array
     */
    protected $bufferTime = [];

    /**
     * 数据包是否JSON格式
     *
     * @var array
     */
    protected $bufferIsJSON = [];

    /**
     * 需要延时关闭的
     *
     * @var array
     */
    protected $delayCloseFd = [];

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

    public static $serverName;

    public function __construct(swoole_server $server, $id)
    {
        require_once __DIR__ .'/FlushData.php';

        $this->server    = $server;
        $this->workerId  = FlushData::$workerId = $id;
        $this->dumpFile  = EtServer::$config['server']['dump_path'] .'total-dump-'. substr(md5(EtServer::$configFile), 16, 8) . '-'. $id .'.txt';
        $this->flushData = new FlushData();

        # 包数据的key
        self::$packKey    = chr(146).chr(206);
        self::$timed      = time();
        $this->startTime  = time();
        self::$serverName = EtServer::$config['server']['host'].':'. EtServer::$config['server']['port'];

    }

    /**
     * 初始化后会调用
     */
    public function init()
    {
        if ($this->isInit)return true;

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
        swoole_timer_tick(3000, function()
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

            # 加载task
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

        # 按进程数为每个 worker 设定一个时间平均分散的定时器
        $limit  = intval(EtServer::$config['server']['merge_time_ms'] ?: 5000);
        $aTime  = intval($limit * $this->workerId / $this->server->setting['worker_num']);
        $mTime  = intval(microtime(1) * 1000);
        $aTime += $limit * ceil($mTime / $limit) - $mTime;

        swoole_timer_after($aTime, function() use ($limit)
        {
            # 推送到task进行数据汇总处理
            swoole_timer_tick($limit, function()
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
            swoole_timer_tick(60000, function()
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
                            $this->closeConnect($fd);
                        }
                        catch (Exception $e){}
                    }
                    $this->delayCloseFd = [];
                }

                # 更新任务
                $this->updateJob();

                if ($this->redis)
                {
                    # 更新监控内存
                    $this->redis->hSet('server.memory', self::$serverName .'_'. $this->workerId, serialize([memory_get_usage(true), self::$timed, self::$serverName, $this->workerId]));
                }
            });

            # 每10分钟处理1次
            swoole_timer_tick(1000 * 600, function()
            {
                # 清理老数据
                if ($this->buffer)
                {
                    self::$timed = time();
                    foreach ($this->buffer as $fd)
                    {
                        if (self::$timed - $this->bufferTime[$fd] > 300)
                        {
                            # 超过5分钟没有更新数据, 则移除
                            info('clear expired data length: '. strlen($this->buffer[$fd]));

                            $this->clearBuffer($fd);
                        }
                    }
                }

                # 更新配置
                $this->reloadSetting();
            });
        });


        # 进程定时重启, 避免数据没清理占用较大内存的情况
        swoole_timer_tick(mt_rand(1000 * 3600 * 2, 1000 * 3600 * 3), function()
        {
            info('now restart worker#'. $this->workerId);
            $this->flush();
            $this->shutdown();
            exit;
        });

        # 只有需要第一个进程处理
        if ($this->workerId == 0)
        {
            # 每3秒通知处理一次
            # 分散投递任务时间
            for ($i = 1; $i < $this->server->setting['task_worker_num']; $i++)
            {
                swoole_timer_after(intval(3000 * $i / $this->server->setting['task_worker_num']), function() use ($i)
                {
                    swoole_timer_tick(3000, function() use ($i)
                    {
                        self::$timed = time();

                        $rs = EtServer::$taskWorkerStatus->get("task{$i}");
                        # $rs['status'] 1表示忙碌, 0表示空闲
                        if (!$rs || !$rs['status'])
                        {
                            # 更新状态
                            EtServer::$taskWorkerStatus->set("task{$i}", ['status' => 1, 'time' => self::$timed]);

                            # 调用任务
                            $this->server->task('job', $i);
                        }
                        elseif ($rs['pid'] && self::$timed - $rs['time'] > 300)
                        {
                            # 5分钟还没反应, 避免极端情况下卡死, 发送一个重启信号, 这种情况下可能会丢失部分数据
                            warn("task worker {$i} is dead, now restart it.");
                            swoole_process::kill($rs['pid']);
                            EtServer::$taskWorkerStatus->del("task{$i}");

                            # 过5秒后处理
                            $pid = $rs['pid'];
                            swoole_timer_after(5000, function() use ($pid)
                            {
                                if (in_array($pid, explode("\n", str_replace(' ', '', trim(`ps -eopid | grep {$pid}`)))))
                                {
                                    # 如果还存在进程, 强制关闭
                                    swoole_process::kill($pid, 9);
                                }
                            });
                        }
                    });
                });
            }

            swoole_timer_tick(1000 * 30, function()
            {
                # 更新服务器信息
                $this->updateServerStatus();
            });

            # 输出到控制台信息
            foreach ($this->queries as $key => $query)
            {
                info("fork sql({$key}): {$query['sql']}");
            }

            # 数据清理
            swoole_timer_tick(1000 * 60 * 5, function()
            {
                $this->server->task('clean', 0);
            });
        }

        return true;
    }


    /**
     * 接受到数据
     *
     * @param swoole_server $server
     * @param $fd
     * @param $fromId
     * @param $data
     * @return bool
     */
    public function onReceive(swoole_server $server, $fd, $fromId, $data)
    {
        if (!$this->redis || $this->pause)
        {
            # 没有连接上redis, 或者是还存在没处理完的数据
            # 关闭连接, 不接受任何数据

            if (isset($this->buffer[$fd]))
            {
                $this->clearBuffer($fd);
            }

            # 如果立即关闭的话, 推送数据的程序会立即重新连接上重新推送数据
            # 所以先放到延迟关闭的数组里, 系统会每1分钟处理1次关闭连接
            $this->delayCloseFd[$fd] = $fd;

            return false;
        }

        if (!isset($this->buffer[$fd]))
        {
            # 包头
            switch (ord($data[0]))
            {
                case 0x5b;      # json 格式的 [ 字符
                    $this->bufferIsJSON[$fd] = true;
                    break;

                case 0x92:      # MsgPack 的3数组
                case 0x93:      # MsgPack 的4数组
                    $this->bufferIsJSON[$fd] = false;
                    break;

                default:
                    warn("accept unknown data length: ". strlen($data). ', head ascii is: '. ord($data[0]));
                    return true;
            }

            $this->buffer[$fd]     = $data;
            $this->bufferTime[$fd] = self::$timed;
        }
        else
        {
            $this->buffer[$fd]    .= $data;
            $this->bufferTime[$fd] = self::$timed;
        }

        # 解开数据
        if ($this->bufferIsJSON[$fd])
        {
            $arr       = $this->unpackByJson($fd);
            $isMsgPack = false;
        }
        else
        {
            $arr       = $this->unpackByMsgPack($fd);
            $isMsgPack = true;
        }

        if (!$arr || !is_array($arr))
        {
            if (($len = strlen(($this->buffer[$fd]))) > 52428800)
            {
                # 超过50MB
                $this->clearBuffer($fd);

                warn("pack data is too long: {$len}byte. now close client.");

                # 关闭连接
                $this->closeConnect($fd);

                return false;
            }

            return true;
        }

        # 处理数据
        $this->execute($fd, $fromId, $arr, $isMsgPack);

        if (!isset($this->buffer[$fd]))return true;

        # 处理粘包的数据
        while(true)
        {
            # 删除上一个引用地址
            unset($arr);

            if ($this->bufferIsJSON[$fd])
            {
                $arr       = $this->unpackByJson($fd);
                $isMsgPack = false;
            }
            else
            {
                $arr       = $this->unpackByMsgPack($fd);
                $isMsgPack = true;
            }

            if (!$arr || !is_array($arr))
            {
                break;
            }

            # 处理数据
            $this->execute($fd, $fromId, $arr, $isMsgPack);

            if (!isset($this->buffer[$fd]))
            {
                break;
            }
        }
        unset($arr);

        return true;
    }

    /**
     * 处理数据
     *
     * @param $fd
     * @param $fromId
     * @param $arr
     * @param $isMsgPack
     * @return bool
     */
    protected function execute($fd, $fromId, & $arr, $isMsgPack)
    {
        $tag = $arr[0];
        if (!$tag || !is_string($tag))
        {
            warn('error data, not found tag');

            # 把客户端关闭了
            $this->closeConnect($fd);
            return false;
        }

        # 查看连接信息
        $info = $this->server->connection_info($fd, $fromId);
        if (false === $info)
        {
            # 连接已经关闭
            warn("connection is closed. tag: {$tag}");
            $this->clearBuffer($fd);
            return false;
        }
        elseif (self::$timed - $info['last_time'] > 30)
        {
            # 最后发送的时间距离现在已经超过 30 秒, 直接不处理, 避免 ack 确认超时的风险
            $this->closeConnect($fd);
            info("connection wait timeout: " . (self::$timed - $info['last_time']) ."s. tag: $tag");
            return false;
        }
        unset($info);

        # example: xd.game.hsqj.consume : $app = hsqj, $table = consume
        # example: consume: $app = '', $table = consume
        list($app, $table) = array_splice(explode('.', $tag), -2);
        if (!$table)
        {
            $table = $app;
            $app   = 'default';
        }

        if (isset($this->jobsGroupByTable[$table]) && $this->jobsGroupByTable[$table])
        {
            # 没有相应tag的任务, 直接跳过
            $haveTask = true;
        }
        else
        {
            $haveTask = false;
        }

        # 是否需要再解析（Fluentd 会把数据通过 buffer 字符串直接传过来）
        $delayParseRecords = $isMsgPack && is_string($arr[1]);

        if ($delayParseRecords || is_array($arr[1]))
        {
            # 多条数据
            # [tag, [[time,record], [time,record], ...], option]
            $option  = $arr[2] ?: [];
            $records = $arr[1];
        }
        else
        {
            # 单条数据
            # [tag, time, record, option]
            $option  = $arr[3] ?: [];
            $records = [[$arr[1], $arr[2]]];
        }

        if ($option && $option['chunk'])
        {
            $ackData = ['ack' => $option['chunk']];
            $isSend  = false;
        }
        else
        {
            $ackData = null;
            $isSend  = true;
        }

        if ($haveTask)
        {
            # 有任务需要处理

            if ($delayParseRecords)
            {
                # 解析数据
                $this->parseRecords($records);
            }

            # 这边的 job 是根据 sql 生成出的数据序列处理任务, 通常情况下是 1个 sql 对应1个序列任务
            # 但2个相同 group by, from 和 where 条件的 sql 则共用一个序列任务, 例如:
            # select count(*) from test group time 1h where id in (1,2,3)
            # 和
            # select sum(value) from test group time 1h where id in (1,2,3) save as test_sum
            # 占用相同序列任务
            #
            # 这样设计的目的是共享相同序列减少数据运算,储存开销

            $jobs = [];
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
                    $count = count($records);

                    if (IS_DEBUG)
                    {
                        debug("worker: $this->workerId, tag: $tag, records count: " . $count);
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
                    warn($e->getMessage());

                    # 重置临时数据
                    $this->flushData->restore();

                    # 关闭连接
                    $this->closeConnect($fd);

                    # 检查连接
                    $this->checkRedis();

                    return false;
                }
            }
        }
        else
        {
            $jobs = null;
        }

        if ($ackData)
        {
            # ACK 确认
            if ($isMsgPack)
            {
                $isSend = $this->server->send($fd, $tmp = msgpack_pack($ackData));
            }
            else
            {
                $isSend = $this->server->send($fd, $tmp = json_encode($ackData));
            }

            if (IS_DEBUG && !$isSend)
            {
                debug("send ack data fail. fd: $fd, data: $tmp");
            }
        }

        if ($jobs)
        {
            if ($isSend)
            {
                # 发送成功

                # 标记为任务完成
                $this->flushData->commit();

                # 计数器增加
                $count = count($records);
                if ($count > 0)
                {
                    EtServer::$counter->add($count);
                }
            }
            else
            {
                # 发送失败, 恢复数据
                $this->flushData->restore();
            }
        }

        return true;
    }

    protected function unpackByJson($fd)
    {
        # JSON 格式数据结尾
        $arr = @json_decode($this->buffer[$fd], true);
        if (!$arr)
        {
            # 处理粘包的可能
            $len = strlen($this->buffer[$fd]);
            $tmp = '';
            for ($i = 0; $i < $len; $i++)
            {
                $tmp .= $this->buffer[$fd][$i];
                if ($this->buffer[$fd][$i] === ']')
                {
                    $arr = @json_decode($tmp, true);
                    if (is_array($arr) && $arr)
                    {
                        $this->buffer[$fd] = substr($this->buffer[$fd], $i + 1);

                        return $arr;
                    }
                }
            }

            return false;
        }
        else
        {
            $this->clearBuffer($fd);

            $countArr = count($arr);
            if ($countArr < 3)
            {
                warn("unknown data, array count mush be 3 or 4, unpack data count is: $countArr");
                $this->closeConnect($fd);

                return false;
            }

            return $arr;
        }
    }

    protected function unpackByMsgPack($fd)
    {
        $arr = @msgpack_unpack($this->buffer[$fd]);

        if (!$arr || !is_array($arr))
        {
            return false;
        }
        else
        {
            if (count($arr) < 2)
            {
                return false;
            }

            $this->clearBuffer($fd);
            return $arr;
        }
    }

    /**
     * 关闭连接
     *
     * @param $fd
     */
    protected function closeConnect($fd)
    {
        $this->clearBuffer($fd);
        $this->server->close($fd);
    }

    protected function clearBuffer($fd)
    {
        unset($this->buffer[$fd]);
        unset($this->bufferTime[$fd]);
        unset($this->bufferIsJSON[$fd]);
    }

    public function onConnect(swoole_server $server, $fd, $fromId)
    {
        if (isset($this->buffer[$fd]))
        {
            $this->clearBuffer($fd);
        }
    }

    public function onClose(swoole_server $server, $fd, $fromId)
    {
        if (isset($this->buffer[$fd]))
        {
            $this->clearBuffer($fd);
        }
    }

    /**
     * @param swoole_server $server
     * @param $fromWorkerId
     * @param $message
     * @return null
     */
    public function onPipeMessage(swoole_server $server, $fromWorkerId, $message)
    {
        switch ($message)
        {
            case 'task.reload':
                # 更新配置
                $this->reloadSetting();
                break;

            case 'pause':
                # 暂停接受任何数据
                $this->pause();
                break;

            case 'continue':
                # 继续接受数据
                $this->stopPause();

                break;
        }
    }

    protected function pause()
    {
        $this->pause        = true;
        $this->buffer       = [];
        $this->bufferIsJSON = [];
        $this->bufferTime   = [];
    }

    protected function stopPause()
    {
        if ($this->delayCloseFd)
        {
            # 有延迟的需要关闭的连接, 先把这些连接全部关闭了
            foreach ($this->delayCloseFd as $fd)
            {
                try
                {
                    $this->closeConnect($fd);
                }
                catch(Exception $e){}
            }
            $this->delayCloseFd = [];
        }

        $this->pause = false;
    }

    public function onFinish($server, $task_id, $data)
    {

    }

    protected function parseRecords(& $recordsData)
    {
        if (is_string($recordsData))
        {
            # 解析里面的数据
            $tmpArr = [];
            $arr    = explode(self::$packKey, $recordsData);
            $len    = count($arr);
            $str    = '';

            for ($i = 1; $i < $len; $i++)
            {
                $str .= self::$packKey . $arr[$i];

                $tmpRecord = @msgpack_unpack($str);
                if (false !== $tmpRecord && is_array($tmpRecord))
                {
                    $recordsData = '';

                    $tmpArr[] = $tmpRecord;

                    # 重置临时字符串
                    $str = '';
                }
            }

            $recordsData = $tmpArr;
        }
    }


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
                if (IS_DEBUG && $this->workerId == 0)
                {
                    debug('redis hosts: '. implode(', ', EtServer::$config['redis']['hosts']));
                }

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

            $this->redis = $redis;

            if (false === $redis->time(0))
            {
                # 大部分用redis的操作, 部分不兼容的用这个对象来处理
                $this->isSSDB = true;
                require_once __DIR__ . '/SSDB.php';

                $this->ssdb = new SimpleSSDB($host, $port);
            }

            $id = null;
            unset($id);

            return true;
        }
        catch (Exception $e)
        {
            if ($this->workerId == 0 && time() % 10 == 0)
            {
                debug($e->getMessage());
                info('redis server is not start, wait start redis://' . (EtServer::$config['redis']['hosts'] ? implode(', ', EtServer::$config['redis']['hosts']) : $host .':'. $port));
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
            info("worker($this->workerId) load {$count} job(s) from file {$this->dumpFile}.");
        }

        # 只需要第一个进程执行
        if ($this->workerId === 0)
        {
            # 如果调小过 task worker num, 需要把之前的 dump 的数据重新 load 回来
            $files = preg_replace('#\-'. $this->workerId .'\.txt$#', '-*.txt', $this->dumpFile);

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
                            info("worker($this->workerId) load {$count} job(s) from file {$file}.");
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
            foreach (explode("\r\n", file_get_contents($file)) as $item)
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
                    warn("load data error: ". $item);
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
                    file_put_contents($this->dumpFile, msgpack_pack($job) . "\r\n", FILE_APPEND);
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
    protected function reloadSetting()
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
                if ($this->workerId == 0)
                {
                    debug("query not use, key: {$opt['key']}, table: {$opt['table']}");
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
                $this->series[$seriesKey] = Manager::createSeriesByQueryOption($opt, $this->queries);

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
                    debug('Worker#' . $this->workerId . " flush {$count} jobs, use time: {$useTime}s" . ($this->flushData->delayCount > 0 ? ", delay jobs: {$this->flushData->delayCount}." : '.'));
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

                    warn('Worker#' . $this->workerId . " is busy. delay jobs: {$this->flushData->delayCount}, now pause accept new data.");
                }
            }
            elseif ($this->pause && $this->autoPause && $this->flushData->delayCount < 20000)
            {
                # 关闭自动暂停
                $this->autoPause  = false;
                $this->stopPause();

                info('Worker#'. $this->workerId .' re-accept new data.');
            }
        }
        catch (Exception $e)
        {
            warn($e->getMessage());

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
                    $value = $data[$item['field']];
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
                                        $value = @date($item['arg'], $value);
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
                                        if (is_callable($item['fun']))
                                        {
                                            try
                                            {
                                                $value = @call_user_func($item['fun'], $value, $item['arg']);
                                            }
                                            catch (Exception $e)
                                            {
                                                $value = false;
                                            }
                                        }
                                        break;
                                }
                                break;
                        }
                    }

                    if (!$isIn)
                    {
                        $rs = self::checkWhereEx($value, $item['value'], $item['type']);
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
