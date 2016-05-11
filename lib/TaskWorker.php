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
            switch ($data)
            {
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
    protected function outputToFluent()
    {
        /**
         * @var Redis $redis
         * @var SimpleSSDB $ssdb
         */
        list($redis, $ssdb) = self::getRedis();
        if (false === $redis)return false;

        getLock:

        $lockKey = 'output_lock';
        if (!$redis->setNx($lockKey, microtime(1)))
        {
            # 抢锁失败
            if (microtime(1) - $redis->get($lockKey) > 600)
            {
                # 如果10分钟还没有释放的锁, 强制删除, 防止被死锁
                $redis->delete($lockKey);

                usleep(mt_rand(1, 1000));

                debug("found and deleted timeout lock.");
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
                # 获取列表
                $keys = $ssdb->hlist('list,', 'list,z' , 20);

                if (!$keys)
                {
                    goto rt;
                }
            }
            else
            {
                # 获取列表
                $keys = $redis->keys('list,*');
                if ($keys)
                {
                    $keys = array_slice($keys, 0, 20);
                }
                else
                {
                    # 没数据
                    goto rt;
                }
            }

            # 加载客户端
            require_once (__DIR__ .'/FluentClient.php');

            # 获取上30秒的时间序列
            $currentLimit = date('YmdHi', time() - 30);
            $outputPrefix = FluentServer::$config['output']['prefix'] ?: '';

            $outData = [];
            sort($keys);
            foreach ($keys as $key)
            {
                if (preg_match('#^list,(?<app>.+),(?<table>.+),(?<limit>\d+)$#', $key, $m))
                {
                    if ($m['limit'] < $currentLimit)
                    {
                        # 只有不在当前时间序列的数据才会处理
                        $data = $redis->hGetAll($key);
                        $tag = "{$outputPrefix}{$m['app']}.{$m['table']}";
                        if (isset($outData[$tag]))
                        {
                            $outData[$tag]['data'] = array_merge($outData[$tag]['data'], $data);
                        }
                        else
                        {
                            $outData[$tag]['data'] = $data;
                        }
                        $outData[$tag]['keys'][] = $key;
                    }
                }
                else
                {
                    # 将不符合格式的key重命名
                    if ($ssdb)
                    {
                        $rs = $redis->hGetAll($key);
                        foreach ($rs as $k => $v)
                        {
                            $redis->hSet("bak.$key", $k, $v);
                        }
                        $ssdb->hclear($key);
                    }
                    else
                    {
                        $redis->rename($key, "bak.$key");
                    }

                    warn("can not match redis key $key, remove it to bak.list.{$key}");
                }
            }

            if ($outData)
            {
                $fluent = new FluentClient(FluentServer::$config['output']['link']);
                foreach ($outData as $tag => $data)
                {
                    # 生成内容
                    $ack    = uniqid('fluent');
                    $buffer =  '["'. $tag .'",[' .implode(',', $data['data']). '], {"chunk":"'. $ack .'"}]';

                    if ($fluent->push_by_buffer($tag, $buffer, $ack))
                    {
                        if ($ssdb)
                        {
                            # 清除成功的key
                            foreach ($data['keys'] as $key)
                            {
                                $ssdb->hclear($key);
                            }
                        }
                        else
                        {
                            # redis支持移除多个key
                            $redis->delete($data['keys']);
                        }
                    }
                    else
                    {
                        debug("push data to ". FluentServer::$config['output']['type'] .': '. FluentServer::$config['output']['link'] . ' fail.');
                    }
                }
            }

        }
        catch (Exception $e)
        {
            $redis->delete($lockKey);
            warn($e->getMessage());

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