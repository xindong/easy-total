<?php

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

    protected static $DataJobs = [];

    public function __construct()
    {
        $this->jobs       = [];
        $this->counter    = [];
        $this->apps       = [];
        $this->counterApp = [];
    }

    /**
     * 结束新数据的标记
     */
    public function commit()
    {
        self::$DataJobs = [];
    }

    /**
     * 将一个任务对象设置备份, 以便在失败时恢复
     *
     * @param $taskKey
     * @param $uniqueId
     * @return DataJob
     */
    public function setBackup($taskKey, $uniqueId)
    {
        if (isset(self::$DataJobs[$uniqueId]))
        {
            # 已经有存档的数据就不用处理了
            return;
        }

        if (!isset($this->jobs[$taskKey][$uniqueId]))
        {
            self::$DataJobs[$uniqueId] = [$taskKey, -1];
        }
        else
        {
            # 将对象克隆出来
            $obj = clone $this->jobs[$taskKey][$uniqueId];
            self::$DataJobs[$uniqueId] = [$taskKey, $obj];
        }
    }

    /**
     * 获取一个任务对象
     *
     * @param $taskKey
     * @param $uniqueId
     * @return DataJob
     */
    public function getJob($taskKey, $uniqueId)
    {
        if (!isset($this->jobs[$taskKey][$uniqueId]))
        {
            $this->jobs[$taskKey][$uniqueId] = new DataJob($uniqueId);
        }

        return $this->jobs[$taskKey][$uniqueId];
    }

    /**
     * 恢复
     */
    public function restore()
    {
        foreach (self::$DataJobs as $uniqueId => $item)
        {
            list($taskKey, $obj) = $item;
            if ($obj === -1)
            {
                unset($this->jobs[$taskKey][$uniqueId]);
            }
            else
            {
                $this->jobs[$taskKey][$uniqueId] = $obj;
            }
        }

        # 清理数据
        $this->commit();
    }

    public function flush($redis)
    {
        /**
         * @var Redis $redis
         */

        # 投递任务处理任务数据
        if ($this->jobs)
        {
            $i    = 0;
            $time = microtime(1);
            while($i < 500)
            {
                if (microtime(1) - $time > 3)break;

                foreach ($this->jobs as $taskKey => $value)
                {
                    # 投递数据, 每次执行不超过3秒钟

                    $taskId = self::getTaskId($taskKey);
                    $j      = 0;
                    foreach ($value as $k => $v)
                    {
                        if (EtServer::$server->task($v, $taskId))
                        {
                            # 投递成功移除对象
                            unset($this->jobs[$taskKey][$k]);
                        }
                        else
                        {
                            # 发送失败可能是缓冲区塞满了, 此时不应该再发送信息了
                            break;
                        }

                        $j++;
                        if ($j === 100)
                        {
                            break;
                        }
                    }

                    if (!$this->jobs[$taskKey])
                    {
                        unset($this->jobs[$taskKey]);
                    }
                }

                if (!$this->jobs)break;

                $i++;
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
        static $cache = [];
        if (isset($cache[$taskKey]))return $cache[$taskKey];

        $taskNum = EtServer::$server->setting['task_worker_num'] - 1;

        if (count($cache) > 200)
        {
            $cache = array_slice($cache, -20, null, true);
        }

        $cache[$taskKey] = (crc32($taskKey) % ($taskNum - 1)) + 1;

        return $cache[$taskKey];
    }
}