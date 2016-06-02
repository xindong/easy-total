<?php

if (class_exists('Thread', false))
{
    class FlushBase extends Thread
    {

    }
}
else
{
    class FlushBase extends ArrayIterator
    {

    }
}

class FlushDataBak
{
    public static $openRestore = false;
    public static $DataUpdated = false;
    public static $DataDist    = [];
    public static $DataTotal   = [];
    public static $DataJobs    = [];
}


/**
 * 数据推送处理的对象
 *
 * 兼容多线程 Thread 的处理方式, 需要安装 pthreads 扩展 see http://php.net/manual/zh/book.pthreads.php
 *
 * Class FlushData
 */
class FlushData extends FlushBase
{
    /**
     * 唯一数据列表
     *
     * @var FlushBase
     */
    public $dist;

    public $counter;

    public $jobs;

    public $total;

    public $apps;

    public $counterApp;

    public $updated;

    public $runTime;

    /**
     * @var Redis
     */
    public static $redis;

    /**
     * @var SimpleSSDB
     */
    public static $ssdb;

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

    public function __construct()
    {
        $this->dist       = new FlushBase();
        $this->counter    = new FlushBase();
        $this->jobs       = new FlushBase();
        $this->total      = new FlushBase();
        $this->apps       = new FlushBase();
        $this->counterApp = new FlushBase();
    }

    /**
     * 开启一个新数据的标记
     */
    public function beginJob()
    {
        FlushDataBak::$openRestore = true;
    }

    /**
     * 结束新数据的标记
     */
    public function endJob()
    {
        FlushDataBak::$openRestore = false;
        FlushDataBak::$DataUpdated = false;
        FlushDataBak::$DataDist    = [];
        FlushDataBak::$DataTotal   = [];
        FlushDataBak::$DataJobs    = [];
    }

    /**
     * 设置是否更新
     *
     * @param $updated
     */
    public function setUpdated($updated)
    {
        if (FlushDataBak::$openRestore)
        {
            FlushDataBak::$DataUpdated = $this->updated;
        }

        $this->updated = $updated;
    }

    public function setDist($key, $hash)
    {
        if (FlushDataBak::$openRestore && !isset($this->dist[$key][$hash]))
        {
            # 原来的数据中没有设置, 则标记下
            FlushDataBak::$DataDist[$key][$hash] = 1;
        }

        $this->dist[$key][$hash] = 1;
    }

    public function setTotal($uniqid, $total)
    {
        if (FlushDataBak::$openRestore && !isset(FlushDataBak::$DataTotal[$uniqid]))
        {

            # 原来的数据中没有设置, 则增加一个
            FlushDataBak::$DataTotal[$uniqid] = isset($this->total[$uniqid]) ? $this->total[$uniqid] : 0;
        }

        $this->total[$uniqid] = $total;
    }

    public function setJobs($key, $id, $value)
    {
        if (FlushDataBak::$openRestore && !isset(FlushDataBak::$DataJobs[$key][$id]))
        {
            FlushDataBak::$DataJobs[$key][$id] = isset($this->jobs[$key][$id]) ? $this->jobs[$key][$id] : 0;
        }

        $this->jobs[$key][$id] = $value;
    }

    /**
     * 恢复
     */
    public function restore()
    {
        $this->updated = FlushDataBak::$DataUpdated;

        # 恢复唯一数值
        foreach (FlushDataBak::$DataDist as $key => $v)
        {
            foreach ($v as $hash => $tmp)
            {
                unset($this->dist[$key][$hash]);
            }
        }

        # 恢复统计数据
        foreach (FlushDataBak::$DataTotal as $uniqid => $v)
        {
            if (0 === $v)
            {
                unset($this->total[$uniqid]);
            }
            else
            {
                $this->total[$uniqid] = $v;
            }
        }

        # 恢复任务数据
        foreach (FlushDataBak::$DataJobs as $key => $value)
        {
            foreach ($value as $id => $v)
            {
                if (0 === $v)
                {
                    unset($this->jobs[$key][$id]);
                    if (!$this->jobs[$key])
                    {
                        unset($this->jobs[$key]);
                    }
                }
                else
                {
                    $this->jobs[$key][$id] = $v;
                }
            }
        }
    }

    /**
     * 执行推送数据操作
     */
    public function run()
    {
        # 线程中处理数据
        $tryNum = 0;
        while (true)
        {
            $this->runTime = time();
            self::doFlush($this);

            # 处理完毕
            if ($this->updated)
            {
                $tryNum++;
                usleep(30000);
            }
            else
            {
                break;
            }
        }
    }

    /**
     * 实施推送数据到redis, ssdb
     *
     * @return bool
     */
    public static function doFlush(FlushData $flushData)
    {
        $redis = self::$redis;
        $ssdb  = self::$ssdb;

        if (!$redis)return false;

        # 更新唯一值
        if ($flushData->dist->count())
        {
            if ($ssdb)
            {
                foreach ($flushData->dist as $k => $v)
                {
                    if (($c = count($v)) > 3000)
                    {
                        # 数据很多
                        $err = false;
                        for ($i = 0; $i < $c; $i += 2000)
                        {
                            if (false === $redis->hMSet($k, array_slice($v, $i, 2000, true)))
                            {
                                $err = true;
                                break;
                            }
                        }
                        if (!$err)
                        {
                            unset($flushData->dist[$k]);
                        }
                    }
                    else
                    {
                        if (false !== $redis->hMSet($k, $v))
                        {
                            # 成功
                            unset($flushData->dist[$k]);
                        }
                    }
                }
            }
            else
            {
                # 使用 Sets 设置
                foreach ($flushData->dist as $k => $v)
                {
                    # 记录下所有唯一值的key列表
                    list($d, $sk) = explode(',', $k, 3);

                    # 记录每个任务的唯一序列, 用于后续数据管理
                    $redis->sAdd("distKeys,{$sk}", $k);

                    $c = count($v);
                    if ($c > 100)
                    {
                        # 超过100个则分批提交
                        $rs   = false;
                        $tmp = [$k];
                        $i    = 0;
                        foreach ($v as $kk => $t)
                        {
                            $i++;
                            $tmp[] = $kk;

                            if ($i % 100 === 0 || $i === $c)
                            {
                                # 每100条提交一次
                                $rs = false !== call_user_func_array([$redis, 'sAdd'], $tmp);
                                if (false === $rs)
                                {
                                    # 有错误, 退出循环
                                    break;
                                }
                            }
                        }
                    }
                    else
                    {
                        $tmp = [$k];
                        foreach ($v as $kk => $t)
                        {
                            $tmp[] = $kk;
                        }
                        $rs = false !== call_user_func_array([$redis, 'sAdd'], $tmp);
                    }
                    unset($tmp);

                    if (false !== $rs)
                    {
                        # 成功
                        unset($flushData->dist[$k]);
                    }
                }
            }
        }

        # 更新任务
        if ($flushData->jobs->count())
        {
            $tryNum    = 0;
            $distCache = [];

            while (true)
            {
                if ($flushData->jobs->count() == 0)break;

                foreach ($flushData->jobs as $jobKey => $arr)
                {
                    $lockKey = "lock,{$jobKey}";

                    # 没用 $redis->set($lockKey, microtime(1), ['nx', 'ex' => 10]); 这样过期设置是因为ssdb不支持
                    if ($redis->setNx($lockKey, microtime(1)))
                    {
                        # 统计用的key
                        list($k1, $k2) = explode(',', date('Ymd,H:i'));

                        # 抢锁成功
                        $beginTime = microtime(1);

                        # 获取序列key
                        list($key, $timeOptKey) = explode(',', $jobKey, 2);

                        # 任务的设置
                        $option    = self::$series[$key];
                        $saveData  = [];

                        if ($option)foreach ($arr as $id => $opt)
                        {
                            list($uniqid, $time, $timeKey, $app, $value) = $opt;

                            # 获取所有统计相关数据
                            $totalKey = "total,{$uniqid}";
                            $total    = $redis->get($totalKey);
                            if (!$total)
                            {
                                $total = [];

                                # 更新数据
                                if (!$ssdb)
                                {
                                    # redis 服务器需要将 key 加入到 totalKeys 里方便后续数据维护
                                    $redis->sAdd("totalKeys,$key", $totalKey);
                                }
                            }
                            else
                            {
                                $total = @unserialize($total) ?: [];
                            }

                            # 更新统计数据
                            if (isset($flushData->total[$uniqid]))
                            {
                                # 合并统计数据
                                $total = self::totalDataMerge($total, $flushData->total[$uniqid], $option['function']);

                                if ($redis->set($totalKey, serialize($total)))
                                {
                                    unset($flushData->total[$uniqid]);
                                }
                            }

                            # 根据查询中的设置设置导出数据内容
                            if (is_array($option['queries'][$timeOptKey]))foreach ($option['queries'][$timeOptKey] as $queryKey)
                            {
                                # 获取查询的配置
                                $queryOption = self::$queries[$queryKey];

                                # 查询已经被移除
                                if (!$queryOption)continue;

                                # 查询已经更改
                                if ($queryOption['seriesKey'] !== $key)continue;

                                # 生成数据
                                $data = [
                                    '_id'    => $id,
                                    '_group' => $timeKey,
                                ];

                                if ($queryOption['allField'])
                                {
                                    $data += $value;
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

                                foreach ($queryOption['fields'] as $as => $saveOpt)
                                {
                                    $field = $saveOpt['field'];
                                    switch ($saveOpt['type'])
                                    {
                                        case 'count':
                                        case 'sum':
                                        case 'min':
                                        case 'max':
                                            $data[$as] = $total[$saveOpt['type']][$field];
                                            break;

                                        case 'first':
                                        case 'last':
                                            $data[$as] = $total[$saveOpt['type']][$field][0];
                                            break;

                                        case 'dist':
                                            $k = "dist,{$uniqid},{$field}";
                                            if (!isset($distCache[$k]))
                                            {
                                                # 获取唯一值的长度
                                                if ($ssdb)
                                                {
                                                    $distCache[$k] = (int)$redis->hLen($k);
                                                }
                                                else
                                                {
                                                    $distCache[$k] = (int)$redis->sCard($k);
                                                }
                                            }
                                            $data[$as] = $distCache[$k];
                                            break;

                                        case 'exclude':
                                            # 排除
                                            unset($data[$as]);
                                            break;

                                        case 'value':
                                        default:
                                            $data[$as] = $value[$field];
                                            break;
                                    }
                                }

                                # 导出的数据key
                                if (is_array($queryOption['saveAs'][$timeOptKey]))
                                {
                                    $tmp = $queryOption['saveAs'][$timeOptKey];
                                    switch ($tmp[1])
                                    {
                                        case 'date':
                                            # 处理时间变量替换
                                            $saveAs = str_replace($tmp[2], explode(',', date($tmp[3], $time)), $tmp[0]);
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

                                # 导出的数据
                                //$saveData[$saveKey][$id] = json_encode([$time, $data], JSON_UNESCAPED_UNICODE);
                                $taskKey = "{$queryKey},{$timeOptKey},{$app},{$saveAs}";

                                $saveData[$taskKey][$taskKey][$id] = json_encode([$time, $data], JSON_UNESCAPED_UNICODE);
                            }
                        }

                        if ($saveData)
                        {
                            $error = false;

                            # 投递任务
                            $taskNum = EtServer::$server->setting['task_worker_num'];
                            foreach ($saveData as $taskKey => $data)
                            {
                                if (false === EtServer::$server->task($data, crc32($taskKey) % $taskNum))
                                {
                                    # 投递失败
                                    $error = true;
                                    break;
                                }
                            }

                            /*
                            if ($ssdb)
                            {
                                foreach ($saveData as $saveKey => $data)
                                {
                                    # ssdb 没有 sAdd 的功能, 所以存到 hash 中
                                    if (false === $redis->hSet('allListKeys', $saveKey, $saveKey) || false === $redis->hMset($saveKey, $data))
                                    {
                                        $error = true;
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                foreach ($saveData as $saveKey => $data)
                                {
                                    if (false === $redis->sAdd('allListKeys', $saveKey) || false === $redis->hMset($saveKey, $data))
                                    {
                                        $error = true;
                                        break;
                                    }
                                }
                            }
                            */

                            # 清除数据释放内存
                            unset($saveData);
                        }
                        else
                        {
                            $error = false;
                        }

                        if (!$error)
                        {
                            # 清除数据
                            unset($flushData->jobs[$jobKey]);
                        }

                        # 释放锁
                        $redis->delete($lockKey);

                        # 使用时间（微妙）
                        $useTime = 1000000 * (microtime(1) - $beginTime);

                        # 更新推送消耗时间统计
                        $redis->hIncrBy("counter.pushtime.$k1.$key", $k2, $useTime);
                    }
                    else if ($tryNum % 100 === 0)
                    {
                        if (microtime(1) - $redis->get($lockKey) > 30)
                        {
                            # 30 秒还没解锁, 直接删除, 防止死锁
                            $redis->delete($lockKey);
                        }
                    }
                }

                if ($flushData->jobs->count())
                {
                    # 重试
                    $tryNum++;
                    usleep(mt_rand(1, 1000));
                }
                else
                {
                    break;
                }
            }
        }


        # 更新APP相关数据
        if ($flushData->apps->count())
        {
            $appData = [];
            foreach ($flushData->apps as $uniqid => $value)
            {
                foreach ($value as $app => $time)
                {
                    # 更新APP设置
                    if (!isset($appData[$app]))
                    {
                        $appData[$app] = $time;
                    }
                }

                # 更新序列对应的APP的最后请求时间
                if (false !== $redis->hMset("series.app.{$uniqid}", $value))
                {
                    unset($flushData->apps[$uniqid]);
                }
            }

            if ($appData)
            {
                $apps = $redis->hMGet('apps', array_keys($appData)) ?: [];
                foreach ($appData as $app => $time)
                {
                    $tmp = @unserialize($apps) ?: [];

                    if (!isset($tmp['firstTime']))
                    {
                        # 初始化一个APP
                        $tmp['name']      = "App:$app";
                        $tmp['firstTime'] = $time;
                    }

                    $tmp['lastTime'] = $time;

                    $apps[$app] = serialize($tmp);
                }

                # 更新APP列表数据
                $redis->hMset('apps', $apps);
            }
        }


        # 同步统计信息
        if ($flushData->counter->count())
        {
            # 按每分钟分开
            foreach ($flushData->counter as $key => $value)
            {
                $allCount = 0;
                foreach ($value as $timeKey => $v)
                {
                    list($k1, $k2) = explode(',', $timeKey);

                    $allCount += $v['total'];

                    # 更新当前任务的当天统计信息
                    $redis->hIncrBy("counter.total.$k1.$key", $k2, $v['total']);
                    $redis->hIncrBy("counter.time.$k1.$key", $k2, $v['time']);
                }

                # 更新任务总的统计信息
                $redis->hIncrBy('counter', $key, $allCount);
                unset($flushData->counter[$key]);
            }
        }

        # 同步APP统计信息
        if ($flushData->counterApp->count())
        {
            foreach ($flushData->counterApp as $app => $value)
            {
                $allCount = 0;
                foreach ($value as $timeKey => $v)
                {
                    list($k1, $k2) = explode(',', $timeKey);

                    $allCount += $v['total'];

                    $redis->hIncrBy("counterApp.total.$k1.$app", $k2, $v['total']);
                    $redis->hIncrBy("counterApp.time.$k1.$app", $k2, $v['time']);
                }

                $redis->hIncrBy('counterApp', $app, $allCount);
                unset($flushData->counterApp[$app]);
            }
        }

        # 标记是否有更新数据
        if ($flushData['dist']
            || $flushData->total->count()
            || $flushData->jobs->count()
            || $flushData->apps->count()
            || $flushData->counter->count()
            || $flushData->counterApp->count()
        )
        {
            $flushData->updated = true;
        }
        else
        {
            $flushData->updated = false;
        }

        return true;
    }


    protected static function totalDataMerge($total, $totalMerge, $fun)
    {
        if (isset($fun['sum']))
        {
            # 相加的数值
            foreach ($fun['sum'] as $field => $t)
            {
                $total['sum'][$field] += $totalMerge['sum'][$field];
            }
        }

        if (isset($fun['count']))
        {
            foreach ($fun['count'] as $field => $t)
            {
                $total['count'][$field] += $totalMerge['count'][$field];
            }
        }

        if (isset($fun['last']))
        {
            foreach ($fun['last'] as $field => $t)
            {
                $tmp = $total['last'][$field];

                if (!$tmp || $tmp[1] < $totalMerge['last'][$field][1])
                {
                    $total['last'][$field] = $totalMerge['last'][$field];
                }
            }
        }

        if (isset($fun['first']))
        {
            foreach ($fun['first'] as $field => $t)
            {
                $tmp = $total['first'][$field];

                if (!$tmp || $tmp[1] > $totalMerge['first'][$field][1])
                {
                    $total['first'][$field] = $totalMerge['first'][$field];
                }
            }
        }

        if (isset($fun['min']))
        {
            foreach ($fun['min'] as $field => $t)
            {
                if (isset($totalMerge['min'][$field]))
                {
                    $total['min'][$field] = min($totalMerge['min'][$field], $total['min'][$field]);
                }
                else
                {
                    $total['min'][$field] = $totalMerge['min'][$field];
                }
            }
        }

        if (isset($fun['max']))
        {
            foreach ($fun['max'] as $field => $t)
            {
                if (isset($totalMerge['max'][$field]))
                {
                    $total['max'][$field] = max($totalMerge['max'][$field], $total['max'][$field]);
                }
                else
                {
                    $total['max'][$field] = $totalMerge['max'][$field];
                }
            }
        }

        return $total;
    }
}