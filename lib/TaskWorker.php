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

    protected static $sendEvents = [];

    public function __construct(swoole_server $server, $id)
    {
        $this->server   = $server;
        $this->id       = $id;
        $this->dumpFile = (EtServer::$config['server']['dump_path'] ?: '/tmp/') . 'total-task-dump-'. substr(md5(EtServer::$configFile), 16, 8) . '-'. $id .'.txt';
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
                    # 每天清理数据
                    $this->clean();

                    info("clean date at ". date('Y-m-d H:i:s'));
                    break;
            }
        }
        catch (Exception $e)
        {
            warn($e->getMessage());
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
                self::checkAckResponse();
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
            $redis->delete($keys);

            # 移除统计数据
            if ($keys = $redis->sMembers("totalKeys,$key"))
            {
                $redis->delete($keys);
            }

            # 移除唯一序列数据
            if ($keys = $redis->sMembers("distKeys,$key"))
            {
                $redis->delete($keys);
            }

            # 移除序列信息
            $redis->delete("series.app.$key");
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
        if (false === $redis)return false;

        # 清理已经删除的任务
        $queries = array_map('unserialize', $redis->hGetAll('queries'));
        foreach ($queries as $key => $query)
        {
            if ($query['deleteTime'] > 0)
            {
                unset($queries[$key]);
                $listKeys = $redis->keys("list,$key,*");
                if ($listKeys)
                {
                    if ($ssdb)
                    {
                        foreach ($listKeys as $listKey)
                        {
                            # 一个个的移除
                            $ssdb->hclear($listKey);
                        }
                    }
                    else
                    {
                        # 批量移除
                        $redis->delete($listKeys);
                    }
                }

                $redis->hDel('queries', $key);
                info("clean data, remove sql({$key}): {$query['sql']}");
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
                            $redis->delete($k);
                        }
                    }

                    $keys = $redis->keys("$k0.$item.$k2.*");
                    if ($keys)
                    {
                        foreach ($keys as $k)
                        {
                            $redis->delete($k);
                        }
                    }
                }
            }

            $redis->delete("counter.allpushtime.$k1");
            $redis->delete("counter.allpushtime.$k2");
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
                self::checkAckResponse();
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
     * 检查ACK返回
     */
    protected static function checkAckResponse()
    {
        foreach (self::$sendEvents as $k => $event)
        {
            list ($key, $data, $time, $tag, $socket, $acks) = $event;

            try
            {
                if ($rs = fread($socket, 10240))
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
                                unset(self::$sendEvents[$k][4][$ack]);

                                if (!$acks)
                                {
                                    # 成功
                                    fclose(@$socket);
                                    unset(self::$sendEvents[$k]);
                                }
                            }
                        }
                    }
                }
                elseif (microtime(1) - $time > 300)
                {
                    # 超时300秒认为失败
                    # 发送新的
                    self::sendToFluent($tag, $key, $data);

                    # 关闭
                    fclose(@$socket);
                    unset(self::$sendEvents[$k]);
                }
            }
            catch (Exception $e)
            {
                warn($e->getMessage());
            }
        }

        self::$sendEvents = array_values(self::$sendEvents);
    }

    /**
     * 将数据发送到Fluent上
     *
     * @param $tag
     * @param $data
     * @return bool
     */
    protected static function sendToFluent($tag, $key, $data)
    {
        $len    = 0;
        $str    = '';
        $link   = EtServer::$config['output']['link'];
        $socket = @stream_socket_client($link, $errno, $errstr, 1, STREAM_CLIENT_CONNECT);
        stream_set_timeout($socket, 0, 1000);

        if (!$socket)
        {
            warn($errstr);
            return false;
        }

        $acks = [];
        foreach ($data as $item)
        {
            $len += strlen($item);
            $str .= $item .',';

            if ($len > 3000000)
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
                self::$sendEvents[] = [$key, $data, microtime(1), $tag, $socket, $acks];
                return true;
            }
            else
            {
                @fclose($socket);
                return false;
            }
        }
        else
        {
            self::$sendEvents[] = [$key, $data, microtime(1), $tag, $socket, $acks];
            return true;
        }
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
            $redis = new Redis();
            $redis->pconnect(EtServer::$config['redis']['host'], EtServer::$config['redis']['port']);

            if (false === $redis->time())
            {
                require_once __DIR__ . '/SSDB.php';
                $ssdb = new SimpleSSDB(EtServer::$config['redis']['host'], EtServer::$config['redis']['port']);
            }

            return [$redis, $ssdb];
        }
        catch (Exception $e)
        {
            return [false, false];
        }
    }
}