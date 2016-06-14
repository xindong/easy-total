<?php

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
 * 这个对象在 task worker 中无用
 *
 * Class FlushData
 */
class FlushData
{
    /**
     * 唯一数据列表
     *
     * @var array
     */
    public $dist;

    /**
     * 统计数据
     *
     * @var array
     */
    public $total;

    /**
     * 计数器
     *
     * @var array
     */
    public $counter;

    /**
     * 任务列表
     *
     * @var array
     */
    public $jobs;

    /**
     * 项目列表
     *
     * @var array
     */
    public $apps;

    /**
     * 项目的计数器
     *
     * @var array
     */
    public $counterApp;

    /**
     * 是否需要更新
     *
     * @var bool
     */
    public $updated;

    protected static $timed;

    public function __construct()
    {
        $this->dist       = [];
        $this->total      = [];
        $this->jobs       = [];
        $this->counter    = [];
        $this->apps       = [];
        $this->counterApp = [];
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

    /**
     * 设置唯一数据
     *
     * @param $taskKey
     * @param $uniqueId
     * @param $value
     */
    public function setDist($taskKey, $uniqueId, $field, $value)
    {
        if (FlushDataBak::$openRestore && !isset($this->dist[$taskKey][$uniqueId][$field][$value]))
        {
            # 原来的数据中没有设置, 则标记下
            FlushDataBak::$DataDist[$uniqueId][$field] = [$value, $taskKey];
        }

        if (!isset($this->dist[$taskKey]))
        {
            $this->dist[$taskKey]            = new DataDist();
            $this->dist[$taskKey][$uniqueId] = new DataObject();
        }

        if (!$this->dist[$taskKey][$uniqueId][$field])
        {
            $this->dist[$taskKey][$uniqueId][$field] = [];
        }

        $this->dist[$taskKey][$uniqueId][$field][$value] = 1;
    }

    /**
     * 设置统计数据
     *
     * @param $taskKey
     * @param $uniqueId
     * @param $value
     */
    public function setTotal($taskKey, $uniqueId, $value)
    {
        if (FlushDataBak::$openRestore && !isset(FlushDataBak::$DataTotal[$taskKey][$uniqueId]))
        {
            # 原来的数据中没有设置, 则增加一个
            FlushDataBak::$DataTotal[$taskKey][$uniqueId] = $this->total[$taskKey][$uniqueId] ?: 0;
        }

        if (!isset($this->total[$taskKey]))
        {
            $this->total[$taskKey] = new DataTotal();
        }

        $this->total[$taskKey][$uniqueId] = $value;
    }

    /**
     * 设置任务数据
     *
     * @param $taskKey
     * @param $uniqueId
     * @param $value
     */
    public function setJobs($taskKey, $uniqueId, $value)
    {
        if (FlushDataBak::$openRestore && !isset(FlushDataBak::$DataJobs[$taskKey][$uniqueId]))
        {
            FlushDataBak::$DataJobs[$taskKey][$uniqueId] = $this->jobs[$taskKey][$uniqueId] ?: 0;
        }

        if (!isset($this->jobs[$taskKey]))
        {
            $this->jobs[$taskKey] = new DataJobs();
        }

        $this->jobs[$taskKey][$uniqueId] = $value;
    }

    /**
     * 恢复
     */
    public function restore()
    {
        $this->updated = FlushDataBak::$DataUpdated;

        # 恢复唯一数值
        foreach (FlushDataBak::$DataDist as $uniqueId => $value)
        {
            foreach ($value as $filed => $v)
            {
                list ($hash, $taskKey) = $v;
                unset($this->dist[$uniqueId][$uniqueId][$hash]);

                if (!count($this->dist[$taskKey][$uniqueId]))
                {
                    unset($this->dist[$taskKey][$uniqueId]);
                }

                if (!count($this->dist[$taskKey]))
                {
                    unset($this->dist[$taskKey]);
                }
            }
        }

        # 恢复统计数据
        foreach (FlushDataBak::$DataTotal as $taskKey => $value)
        {
            foreach ($value as $uniqueId => $v)
            {
                if (0 === $v)
                {
                    unset($this->total[$taskKey][$uniqueId]);
                    if (!count($this->total[$taskKey]))
                    {
                        unset($this->total[$taskKey]);
                    }
                }
                else
                {
                    $this->total[$taskKey][$uniqueId] = $v;
                }
            }
        }

        # 恢复任务数据
        foreach (FlushDataBak::$DataJobs as $taskKey => $value)
        {
            foreach ($value as $uniqueId => $v)
            {
                if (0 === $v)
                {
                    unset($this->jobs[$taskKey][$uniqueId]);
                    if (!count($this->jobs[$taskKey]))
                    {
                        unset($this->jobs[$taskKey]);
                    }
                }
                else
                {
                    $this->jobs[$taskKey][$uniqueId] = $v;
                }
            }
        }

        # 清理数据
        $this->endJob();
    }

    public function flush($redis)
    {
        /**
         * @var Redis $redis
         */

        # 投递任务处理唯一值
        if ($this->dist)foreach ($this->dist as $taskKey => $value)
        {
            # 投递数据
            if (EtServer::$server->task($value, self::getTaskId($taskKey)))
            {
                # 投递成功移除对象
                unset($this->dist[$taskKey]);
            }
        }

        # 投递任务处理任务数据
        if ($this->jobs)foreach ($this->jobs as $taskKey => $value)
        {
            # 投递数据
            if (EtServer::$server->task($value, self::getTaskId($taskKey)))
            {
                # 投递成功移除对象
                unset($this->jobs[$taskKey]);
            }
        }

        # 投递任务处理统计数据
        if ($this->total)foreach ($this->total as $taskKey => $value)
        {
            # 投递数据
            if (EtServer::$server->task($value, self::getTaskId($taskKey)))
            {
                # 投递成功移除对象
                unset($this->total[$taskKey]);
            }
        }

        if (!$redis)return;

        # 更新APP相关数据
        if ($this->apps)
        {
            $appData = [];
            foreach ($this->apps as $uniqueId => $value)
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
                if (false !== $redis->hMset("series.app.{$uniqueId}", $value))
                {
                    unset($this->apps[$uniqueId]);
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
        if ($this->counter)
        {
            # 按每分钟分开
            foreach ($this->counter as $key => $value)
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
                unset($this->counter[$key]);
            }
        }

        # 同步APP统计信息
        if ($this->counterApp)
        {
            foreach ($this->counterApp as $app => $value)
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
                unset($this->counterApp[$app]);
            }
        }

        # 标记是否有更新数据
        if ($this->jobs || $this->dist || $this->total || $this->counter || $this->counterApp || $this->apps)
        {
            $this->updated = true;
        }
        else
        {
            $this->updated = false;
        }

        return;
    }

    /**
     * 根据任务key获取taskId
     *
     * 不分配id = 0的任务
     *
     * @param $taskKey
     * @return int
     */
    public static function getTaskId($taskKey)
    {
        $taskNum = EtServer::$server->setting['task_worker_num'] - 1;

        return (crc32($taskKey) % ($taskNum - 1)) + 1;
    }

    /**
     * 统计数据
     *
     * @param $total
     * @param $item
     * @param $fun
     * @param $time
     * @return array
     */
    public static function totalData($total, $item, $fun, $time)
    {
        if (!$total)$total = new DataTotalItem();

        if (isset($fun['sum']))
        {
            # 相加的数值
            foreach ($fun['sum'] as $field => $t)
            {
                $total->sum[$field] += $item[$field];
            }
        }

        if (isset($fun['count']))
        {
            foreach ($fun['count'] as $field => $t)
            {
                $total->count[$field] += 1;
            }
        }

        if (isset($fun['last']))
        {
            foreach ($fun['last'] as $field => $t)
            {
                $tmp = $total->last[$field];

                if (!$tmp || $tmp[1] < $time)
                {
                    $total->last[$field] = [$item[$field], $time];
                }
            }
        }

        if (isset($fun['first']))
        {
            foreach ($fun['first'] as $field => $t)
            {
                $tmp = $total->first[$field];

                if (!$tmp || $tmp[1] > $time)
                {
                    $total->first[$field] = [$item[$field], $time];
                }
            }
        }

        if (isset($fun['min']))
        {
            foreach ($fun['min'] as $field => $t)
            {
                if (isset($total->min[$field]))
                {
                    $total->min[$field] = min($total['min'][$field], $item[$field]);
                }
                else
                {
                    $total->min[$field] = $item[$field];
                }
            }
        }

        if (isset($fun['max']))
        {
            foreach ($fun['max'] as $field => $t)
            {
                if (isset($total->max[$field]))
                {
                    $total->max[$field] = max($total['max'][$field], $item[$field]);
                }
                else
                {
                    $total->max[$field] = $item[$field];
                }
            }
        }

        return $total;
    }
}