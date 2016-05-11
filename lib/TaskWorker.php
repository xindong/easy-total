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
        switch ($data)
        {
            case 'output':
                $ssdb  = null;
                $redis = new Redis();
                $redis->pconnect(FluentServer::$config['redis']['host'], FluentServer::$config['redis']['port']);

                if (false === $redis->time())
                {
                    require_once __DIR__ . '/SSDB.php';
                    $ssdb = new SimpleSSDB(FluentServer::$config['redis']['host'], FluentServer::$config['redis']['port']);
                }

                $this->outputToFluent($redis, $ssdb);

                $redis->close();
                break;
            case 'clean':
                # 每天清理数据
                $this->clean();
                break;
        }
    }

    public function shutdown()
    {

    }


    /**
     * 清理redis中的数据
     *
     * @param $key
     */
    public function clearDataByKey($key)
    {
        if ($this->isSSDB)
        {
            while ($keys = $this->ssdb->hlist("total,{$key},", "total,{$key},z", 100))
            {
                foreach ($keys as $k)
                {
                    $this->ssdb->hclear($k);
                }
            }

            while ($keys = $this->ssdb->hlist("dist,{$key},", "dist,{$key},z", 100))
            {
                foreach ($keys as $k)
                {
                    $this->ssdb->hclear($k);
                }
            }

            while ($keys = $this->ssdb->hlist("join,{$key},", "dist,{$key},z", 100))
            {
                foreach ($keys as $k)
                {
                    $this->ssdb->hclear($k);
                }
            }
        }
        else
        {
            $keys = $this->redis->keys("total,{$key},*");
            if ($keys)
            {
                $this->redis->delete($keys);
            }

            $keys = $this->redis->keys("dist,{$key},*");
            if ($keys)
            {
                $this->redis->delete($keys);
            }

            $keys = $this->redis->keys("join,{$key},*");
            if ($keys)
            {
                $this->redis->delete($keys);
            }
        }
    }

    protected function clean()
    {
        # 清理每天的统计数据, 只保留10天内的
        $time = time();
        $k1   = date('Y-m-d', $time - 86400 * 11);
        $k2   = date('Y-m-d', $time - 86400 * 12);
        if ($this->isSSDB)
        {
            while ($keys = $this->ssdb->hlist("counter.total.$k1", "counter.total.$k2", 100))
            {
                # 列出key
                foreach ($keys as $k)
                {
                    # 清除
                    $this->ssdb->hclear($k);
                }
            }
            while ($keys = $this->ssdb->hlist("counter.time.$k1", "counter.time.$k2", 100))
            {
                # 列出key
                foreach ($keys as $k)
                {
                    # 清除
                    $this->ssdb->hclear($k);
                }
            }
        }
        else
        {
            # 获取所有key
            $keys = $this->redis->keys("counter.total.$k1.*");
            if ($keys)foreach ($keys as $k)
            {
                $this->redis->delete($k);
            }

            $keys = $this->redis->keys("counter.time.$k1.*");
            if ($keys)foreach ($keys as $k)
            {
                $this->redis->delete($k);
            }

            $keys = $this->redis->keys("counter.total.$k2.*");
            if ($keys)foreach ($keys as $k)
            {
                $this->redis->delete($k);
            }

            $keys = $this->redis->keys("counter.time.$k2.*");
            if ($keys)foreach ($keys as $k)
            {
                $this->redis->delete($k);
            }
        }
    }

    /**
     * 将数据重新分发到 Fluent
     *
     * @return bool
     */
    protected function outputToFluent(Redis $redis, $ssdb = null)
    {
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

        return true;
    }
}