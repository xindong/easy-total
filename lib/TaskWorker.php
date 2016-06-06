<?php

class TaskWorker
{
    /**
     * @var int
     */
    protected $id;

    /**
     * @var swoole_server
     */
    protected $server;

    /**
     * 当程序需要终止时如果无法把数据推送出去时临时导出的文件路径（确保数据安全）
     *
     * @var string
     */
    protected $dumpFile;

    /**
     * 待推送列表数据
     *
     * @var array
     */
    protected $list = [];

    /**
     * 当前进程启动时间
     *
     * @var int
     */
    protected $startTime;

    protected static $sendEvents = [];


    public function __construct(swoole_server $server, $id)
    {
        $this->server    = $server;
        $this->id        = $id;
        $this->dumpFile  = (EtServer::$config['server']['dump_path'] ?: '/tmp/') . 'total-task-dump-'. substr(md5(EtServer::$configFile), 16, 8) . '-'. $id .'.txt';
        $this->startTime = time();
    }

    public function init()
    {
        if (is_file($this->dumpFile))
        {
            $tmp = @unserialize(file_get_contents($this->dumpFile));
            if ($tmp && is_array($tmp))
            {
                $this->list = $tmp;
            }

            unlink($this->dumpFile);
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
            if (is_array($data))
            {
                $type = 'list';
            }
            else
            {
                $data = explode('|', $data);
                $type = $data[0];
            }

            switch ($type)
            {
                case 'list':
                    # 处理列表数据
                    foreach ($data as $key => $item)
                    {
                        if (isset($this->list[$key]))
                        {
                            $this->list[$key] = array_merge($this->list[$key], $item);
                        }
                        else
                        {
                            $this->list[$key] = $item;
                        }
                    }

                    break;

                case 'output':
                    $this->outputToFluent();
                    break;

                case 'clean':
                    # 每小时清理数据
                    $this->clean();

                    info("clean date at ". date('Y-m-d H:i:s'));
                    break;
            }
        }
        catch (Exception $e)
        {
            warn($e->getMessage());
        }

        # 标记状态为成功
        $this->updateStatus(1);

        # 如果启动超过1小时
        if (time() - $this->startTime > 3600)
        {
            if (mt_rand(1, 100) === 1)
            {
                # 重启进程避免数据溢出,未清理数据暂用超大内存
                $this->shutdown();

                info('now restart task worker: '. $this->id);

                exit(0);
            }
        }
    }

    public function shutdown()
    {
        if ($this->list)
        {
            # 如果有数据推送
            $this->outputToFluent();
        }

        if ($this->list)
        {

            # 把数据dump到本地, 重新启动时加载
            $this->dumpData();
        }

        $time = time();
        while (true)
        {
            if (self::$sendEvents)
            {
                self::checkAck();
            }
            else
            {
                break;
            }

            if (time() - $time > 200)
            {
                if (self::$sendEvents)
                {
                    # 将推送失败的数据恢复后dump出来, 供下次启动时读取
                    foreach (self::$sendEvents as $item)
                    {
                        list ($key, $data) = $item;
                        if (!isset($this->list[$key]))
                        {
                            $this->list[$key] = $data;
                        }
                        else
                        {
                            $this->list[$key] = array_merge($data, $this->list[$key]);
                        }
                    }
                    $this->dumpData();
                }
                break;
            }

            # 更新状态
            $this->updateStatus();

            # update title
            EtServer::setProcessName("php easy-total task wait ack update at ". date('H:i:s') ." [do not kill me]");

            usleep(100000);
        }
    }


    /**
     * 在程序退出时保存数据
     */
    public function dumpData()
    {
        if ($this->list)
        {
            # 有数据
            file_put_contents($this->dumpFile, serialize($this->list));
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
        /**
         * @var Redis $redis
         * @var SimpleSSDB $ssdb
         */
        list($redis, $ssdb) = self::getRedis();

        if (!FlushData::$series && $redis)
        {
            # 获取所有序列的配置
            FlushData::$series = array_map('unserialize', $redis->hGetAll('series'));
        }

        # 清理过期的统计信息, EtServer::$totalTable 里可能有上万甚至更多数据
        $delKeys  = [];
        $time     = time();
        $oldCount = count(EtServer::$totalTable);
        $index    = 0;
        foreach (EtServer::$totalTable as $key => $item)
        {
            $index++;
            if (preg_match('#(?<seriesKey>[0-9a-z]+),(?<limit>\d+)(?<type>[a-z]+),(?<app>[a-z0-9_\-]+),(?<timeKey>\d+)_#i', $key, $m))
            {
                if ($index % 1000 === 0)
                {
                    # 每1条更新下
                    $time = time();
                    $this->updateStatus();
                }

                $seriesOption = FlushData::$series[$m['seriesKey']] ?: [];
                $table        = $seriesOption['table'];
                $logTime      = EtServer::$logTimeTable->get("{$m['app']}_{$table}");
                if (!$logTime)
                {
                    # 如果没有获取到最后的log的时间
                    $logTime = $time - 1800;
                }

                # 根据当前分组获取下一个时间点的时间戳
                $nextTime = self::getNextTimestampByTimeKey($m['timeKey'], $m['limit'], $m['type']);

                if ($oldCount > 100000 && $logTime - $nextTime >= 600)
                {
                    # 当已记录的数据比较庞大, 则清理过期数据
                    $delKeys[] = $key;
                    EtServer::$totalTable->del($key);
                }
                elseif ($redis && $time - $item['time'] < 300)
                {
                    # 5分钟内更新的数据, 同步到 redis
                    $redis->set($key, $item['value']);
                }
            }
            else
            {
                $delKeys[] = $key;
                EtServer::$totalTable->del($key);
                warn("unexpected total key: $key");
            }
        }

        $currentCount = count(EtServer::$totalTable);
        $cleanCount   = $currentCount - $oldCount;
        if ($cleanCount)
        {
            info("clean " . $cleanCount . " total item, current total item count: {$currentCount}.");
        }

        static $lastClean = null;
        if (null === $lastClean)
        {
            $lastClean = time();
        }

        if (time() - $lastClean > 3600)
        {
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
        $this->updateStatus();

        /**
         * @var Redis $redis
         * @var SimpleSSDB $ssdb
         */
        if (false === $redis)return false;

        $time = time();
        foreach (EtServer::$logTimeTable as $k => $v)
        {
            if ($time - $v['update'] > 86400)
            {
                # 清理1天还没有更新的数据
                EtServer::$logTimeTable->del($k);
            }
            elseif ($time - $v['update'] < 3660)
            {
                # 在1小时内有更新的数据更新到redis里
                $redis->set('logTime', $k, $v['time'] . ',' . $v['update']);
            }
        }

        if (time() - $time > 3)
        {
            self::updateStatus();
        }

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
        FlushData::$series = $series;
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
                    $this->updateStatus();
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
     * 将数据重新分发到 Fluent
     *
     * @return bool
     */
    protected function outputToFluent()
    {
        # 加载客户端
        $outputPrefix = EtServer::$config['output']['prefix'] ?: '';

        try
        {
            if (self::$sendEvents)
            {
                self::checkAck();
            }

            foreach ($this->list as $key => $item)
            {
                if (!preg_match('#^(?<jobKey>[a-z0-9]+),(?<timeType>[a-z0-9]+),(?<app>.+),(?<table>.+)$#i', $key, $m))
                {
                    unset($this->list[$key]);
                    warn("Unexpected key: $key");
                    continue;
                }

                $tag = "{$outputPrefix}{$m['app']}.{$m['table']}";;

                # 发送数据
                if (self::sendToFluent($tag, $key, $item))
                {
                    unset($this->list[$key]);
                }
                else
                {
                    warn("push data {$key} fail. fluentd server: " . EtServer::$config['output']['type'] . ': ' . EtServer::$config['output']['link']);
                }

                # 更新状态
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
     * 更新状态
     *
     * @param int $status 1 - 成功, 2 - 运行中
     */
    protected function updateStatus($status = 2)
    {
        EtServer::$taskWorkerStatusTable->set("task{$this->id}", ['status' => $status, 'time' => time()]);
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
                list ($key, $data, $tag, $retryNum) = $event;

                # 移除当前的对象
                unset(self::$sendEvents[$k]);

                if ($data)
                {
                    # 切分成2分重新发送
                    $len = ceil(count($data) / 2);
                    if ($len > 1)
                    {
                        self::sendToFluent($tag, $key, array_slice($data, 0, $len), $retryNum + 1);
                        self::sendToFluent($tag, $key, array_slice($data, $len), $retryNum + 1);
                    }
                    else
                    {
                        self::sendToFluent($tag, $key, $data, $retryNum + 1);
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
        list ($key, $data, $tag, $retryNum, $time, $socket, $acks) = $event;

        try
        {
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

                warn("get ack response timeout, tag: {$tag}, key: {$key}, retryNum: {$retryNum}");

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
     * @param string $key
     * @param array $data
     * @param int $retryNum
     * @return bool
     */
    protected static function sendToFluent($tag, $key, $data, $retryNum = 0)
    {
        $link   = EtServer::$config['output']['link'];
        $socket = @stream_socket_client($link, $errno, $errstr, 1, STREAM_CLIENT_CONNECT);
        stream_set_timeout($socket, 0, 3000);

        if (!$socket)
        {
            warn($errstr);
            return false;
        }

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
                $buffer =  '["'. $tag .'",['. substr($str, 0, -1) .'], {"chunk":"'. $ack .'"}]';

                if (@fwrite($socket, $buffer))
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
            $buffer = '["'. $tag .'",['. substr($str, 0, -1) .'], {"chunk":"'. $ack .'"}]';

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

        $event = [$key, $data, $tag, $retryNum, microtime(1), $socket, $acks];

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

    function getNextTimestampByTimeKey($timeKey, $limit, $type)
    {
        static $cache = [];
        $key = "$timeKey$limit$type";

        if (isset($cache[$key]))return $cache[$key];
        $year     = intval(substr($timeKey, 0, 4));
        $nextYear = strtotime(($year + 1) .'-01-01 00:00:00');
        switch ($type)
        {
            case 'm':
                # 月
                preg_match('#(\d{4})(\d{2})#', $timeKey, $m);
                $month = $limit + $m[2];
                if ($month > 12)
                {
                    $m[1] += 1;
                    $month = 1;
                }

                $time = strtotime("{$year}-{$month}-01 00:00:00");
                $time = min($nextYear, $time);
                break;

            case 'w':
                # 周
                preg_match('#(\d{4})(\d{2})#', $timeKey, $m);
                $time = strtotime("{$year}-01-01 00:00:00") + ($m[2] + $limit) * (86400 * 7);
                $time = min($nextYear, $time);
                break;

            case 'd':
                preg_match('#(\d{4})(\d{3})#', $timeKey, $m);
                var_dump($limit);
                $time = strtotime("{$year}-01-01 00:00:00") + ($m[2] - 1 + $limit) * 86400;
                $time = min($nextYear, $time);
                break;

            case 'M':
            case 'i':
                # 分钟 201604100900
                preg_match('#(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})#', $timeKey, $m);
                $time  = strtotime("{$year}-{$m[2]}-{$m[3]} {$m[4]}:00:00");
                $time += min(3600, 60 * ($m[5] + $limit));
                break;

            case 's':
                # 秒 20160410090000
                preg_match('#(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})#', $timeKey, $m);
                $time  = strtotime("{$year}-{$m[2]}-{$m[3]} {$m[4]}:{$m[5]}:00");
                $time += min(60, ($m[6] + $limit));
                break;

            case 'h':
            default:
                # 小时 2016041000
                preg_match('#(\d{4})(\d{2})(\d{2})(\d{2})#', $timeKey, $m);
                $time  = strtotime("{$year}-{$m[2]}-{$m[3]} 00:00:00");
                $time += min(86400, 3600 * ($m[4] + $limit));

                break;
        }

        if (count($cache) > 100)
        {
            $cache = array_slice($cache, -10, null, true);
        }

        $cache[$key] = $time;
        return $time;
    }
}