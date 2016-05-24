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
        if (MULTI_THREADED_MODE)
        {
            # 多线程模式
            $this->startThread();
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
            $arr = explode('|', $data);
            switch ($arr[0])
            {
                case 'output':
                    # 主进程会同时推送很多任务过来, 这样可以错开处理
                    usleep(mt_rand(100, 300000));
                    $this->outputToFluent($arr[1]);
                    $this->server->finish('output.finish|'.$arr[1]);
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
     * 开启多线程
     */
    protected function startThread()
    {

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
                # $keys = $ssdb->hlist("list,{$jobKey},", "list,{$jobKey},z" , 30);
                $keys = $ssdb->hgetall('allListKeys');
            }
            else
            {
                # 获取列表
                $keys = $redis->sMembers('allListKeys');
            }

            if (!$keys)
            {
                # 没数据
                goto rt;
            }

            # 排序, 便于下面整理
            asort($keys);

            # 整理key
            $currentLimit = date('YmdHi', time() - 30);
            $outputPrefix = Server::$config['output']['prefix'] ?: '';
            $myKeys       = [];
            foreach ($keys as $key)
            {
                if (preg_match('#^list,(?<jobKey>[a-z0-9]+),(?<timeType>[a-z0-9]+),(?<limit>\d+),(?<app>.+),(?<table>.+)$#i', $key, $m))
                {
                    # 不是当前任务
                    if ($m['jobKey'] !== $jobKey)continue;

                    # 新的格式, 将时间参数放在前面, 并加入 jobKey
                    if ($m['limit'] < $currentLimit)
                    {
                        # 得到按时间分组的序列key
                        preg_match('#^(\d+)([a-z]+)$#', $m['timeType'], $mm);
                        $timeGroup = getTimeKey(strtotime($m['limit']), $mm[1], $mm[2]);
                        $delayKey  = "{$m['timeType']},{$timeGroup},{$m['app']},{$m['table']}";

                        $myKeys[$delayKey]['tag']    = "{$outputPrefix}{$m['app']}.{$m['table']}";
                        $myKeys[$delayKey]['limit']  = $m['limit'];
                        $myKeys[$delayKey]['keys'][] = $key;
                    }
                }
            }

            if (!$myKeys)
            {
                goto rt;
            }

            # 加载客户端
            require_once (__DIR__ .'/FluentClient.php');
            $fluent = new FluentClient(Server::$config['output']['link']);

            $lastSyncTime = time();
            foreach ($myKeys as $groups)
            {
                $tag  = $groups['tag'];
                $keys = $groups['keys'];

                if (count($keys) > 1)
                {
                    $data = [];
                    foreach ($keys as $key)
                    {
                        # 数据合并
                        $data = array_merge($data, $redis->hGetAll($key) ?: []);
                    }
                }
                else
                {
                    $key  = $keys[0];
                    $data = $redis->hGetAll($key);
                }

                if ((!$data && false !== $redis->ping()) || self::sendToFluent($fluent, $tag, $data))
                {
                    # 成功后移除当前key的数据
                    if ($ssdb)
                    {
                        foreach ($keys as $key)
                        {
                            $ssdb->hdel('allListKeys', $key);
                            $ssdb->hclear($key);
                        }
                    }
                    else
                    {
                        # 删除数据
                        if (false !== $redis->delete($keys))
                        {
                            # 在列表中移除
                            if (count($keys) < 20)
                            {
                                # 批量删除
                                call_user_func_array([$redis, 'sRemove'], array_merge(['allListKeys'], $keys));
                            }
                            else
                            {
                                foreach ($keys as $key)
                                {
                                    $redis->sRemove('allListKeys', $key);
                                }
                            }
                        }
                    }

                    debug("output data {$jobKey} time limit {$groups['limit']}");
                }
                else
                {
                    warn("push data {$keys} fail. fluentd server: ". Server::$config['output']['type'] .': '. Server::$config['output']['link']);
                }

                if (time() - $lastSyncTime > 5)
                {
                    # 超过10秒钟则更新锁时间值并通知worker进程继续执行
                    $this->server->finish('output.continue|'. $jobKey);

                    # 更新锁信息
                    $redis->set($lockKey, $lockTime = microtime(1));

                    # 更新上次同步时间
                    $lastSyncTime = time();
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

            if ($len > 3000000)
            {
                # 每 3M 分开一次推送, 避免一次发送的数据包太大
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
            $buffer = '["'. $tag .'",['. substr($str, 0, -1) .'], {"chunk":"'. $ack .'"}]';

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
            $redis->pconnect(Server::$config['redis']['host'], Server::$config['redis']['port']);

            if (false === $redis->time())
            {
                require_once __DIR__ . '/SSDB.php';
                $ssdb = new SimpleSSDB(Server::$config['redis']['host'], Server::$config['redis']['port']);
            }

            return [$redis, $ssdb];
        }
        catch (Exception $e)
        {
            return [false, false];
        }
    }
}