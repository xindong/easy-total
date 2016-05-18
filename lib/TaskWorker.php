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

    public function __construct(swoole_server $server, $id)
    {
        $this->server   = $server;
        $this->id       = $id;
    }

    public function init()
    {

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
            $arr = explode('|', $data);
            switch ($arr[0])
            {
                case 'output':
                    $this->outputToFluent($arr[1]);
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
        # 没有什么需要处理的
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


        if ($ssdb)
        {
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
            foreach (['total', 'dist', 'join'] as $item)
            {
                $keys = $redis->keys("{$item},{$key},*");
                if ($keys)
                {
                    $redis->delete($keys);
                }
            }

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
    protected function outputToFluent($jobKey)
    {
        /**
         * @var Redis $redis
         * @var SimpleSSDB $ssdb
         */
        list($redis, $ssdb) = self::getRedis();
        if (false === $redis)return false;

        getLock:

        $lockKey = "output_lock_{$jobKey}";
        if (!$redis->setNx($lockKey, $lockTime = microtime(1)))
        {
            # 抢锁失败
            if ($lockTime - $redis->get($lockKey) > 60)
            {
                # 如果1分钟还没有释放的锁, 强制删除, 防止被死锁
                $redis->delete($lockKey);

                usleep(mt_rand(1, 1000));

                debug("found and deleted timeout lock: $lockKey.");
                goto getLock;
            }
            else
            {
                return false;
            }
        }

        try
        {
            if ($ssdb)
            {
                # 读取当前任务的前30个列表
                $keys = $ssdb->hlist("list,{$jobKey},", "list,{$jobKey},z" , 30);

                if (!$keys)
                {
                    goto rt;
                }
            }
            else
            {
                # 获取列表
                $keys = $redis->keys("list,{$jobKey},*");
                if ($keys)
                {
                    $keys = array_slice($keys, 0, 30);
                }
                else
                {
                    # 没数据
                    goto rt;
                }
            }

            # 加载客户端
            require_once (__DIR__ .'/FluentClient.php');
            $fluent = new FluentClient(FluentServer::$config['output']['link']);

            # 获取上60秒的时间序列
            $currentLimit = date('YmdHi', time() - 60);
            $outputPrefix = FluentServer::$config['output']['prefix'] ?: '';

            # 遍历key
            foreach ($keys as $key)
            {
                if (preg_match('#^list,(?<jobKey>[a-z0-9]+),(?<timeType>[a-z0-9]+),(?<limit>\d+),(?<app>.+),(?<table>.+)$#i', $key, $m))
                {
                    # 新的格式, 将时间参数放在前面, 并加入 jobKey
                    if ($m['limit'] < $currentLimit)
                    {
                        # 读取数据
                        $data = $redis->hGetAll($key);
                        $tag  = "{$outputPrefix}{$m['app']}.{$m['table']}";

                        # 开始推送
                        if (self::sendToFluent($fluent, $tag, $data))
                        {
                            # 成功后移除当前key的数据
                            if ($ssdb)
                            {
                                $ssdb->hclear($key);
                            }
                            else
                            {
                                $redis->delete($key);
                            }
                        }
                        else
                        {
                            debug("push data {$tag} fail. fluentd server: tcp://". FluentServer::$config['output']['type'] .': '. FluentServer::$config['output']['link']);
                        }
                    }
                }
                else
                {
                    # 将不符合格式的key重命名
                    if ($ssdb)
                    {
//                        $rs = $redis->hGetAll($key);
//                        foreach ($rs as $k => $v)
//                        {
//                            $redis->hSet("bak.$key", $k, $v);
//                        }
//                        $ssdb->hclear($key);
                    }
                    else
                    {
//                        $redis->rename($key, "bak.$key");
                    }

                    warn("can not match redis key $key, remove it to bak.list.{$key}");
                }

                $useTime = time() - $lockTime;

                if ($useTime > 60)
                {
                    throw new Exception('use too much time.');
                }
                elseif ($useTime > 10)
                {
                    # 更新锁时间值
                    $redis->set($lockKey, $lockTime = microtime(1));
                }
            }

            # 关闭对象
            $fluent->close();
        }
        catch (Exception $e)
        {
            warn($e->getMessage());

            if (isset($fluent))
            {
                $fluent->close();
            }

            # 释放锁
            $redis->delete($lockKey);

            # 关闭连接
            $redis->close();
            if ($ssdb)$ssdb->close();

            return false;
        }

        rt:
        # 释放锁
        $redis->delete($lockKey);

        # 关闭连接
        $redis->close();
        if ($ssdb)$ssdb->close();

        return true;
    }

    protected static function sendToFluent(FluentClient $fluent, $tag, $data)
    {
        $len = 0;
        $str = '';

        foreach ($data as $item)
        {
            $len += strlen($item);
            $str .= $item .',';

            if ($len > 2000000)
            {
                # 每 2M 分开一次推送, 避免一次发送的数据包太大
                $ack    = uniqid('fluent');
                $buffer =  '["'. $tag .'",['. substr($str, 0, -1) .'], {"chunk":"'. $ack .'"}]';
                if ($fluent->push_by_buffer($tag, $buffer, $ack))
                {
                    # 重置后继续
                    $len = 0;
                    $str = '';
                }
                else
                {
                    # 如果推送失败
                    $fluent->close();
                    return false;
                }
            }
        }

        if ($len > 0)
        {
            $ack    = uniqid('fluent');
            $buffer =  '["'. $tag .'",['. substr($str, 0, -1) .'], {"chunk":"'. $ack .'"}]';

            if ($fluent->push_by_buffer($tag, $buffer, $ack))
            {
                # 全部推送完毕
                return true;
            }
            else
            {
                $fluent->close();
                return false;
            }
        }
        else
        {
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
            $redis->pconnect(FluentServer::$config['redis']['host'], FluentServer::$config['redis']['port']);

            if (false === $redis->time())
            {
                require_once __DIR__ . '/SSDB.php';
                $ssdb = new SimpleSSDB(FluentServer::$config['redis']['host'], FluentServer::$config['redis']['port']);
            }

            return [$redis, $ssdb];
        }
        catch (Exception $e)
        {
            return [false, false];
        }
    }
}