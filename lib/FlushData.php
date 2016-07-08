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
     * 已经到了推送的时间但是没有推送的任务数
     *
     * @var int
     */
    public $delayCount = 0;

    /**
     * 计数器
     *
     * @var array
     */
    public $counter = [];

    /**
     * 任务列表
     *
     * @var array
     */
    public $jobs = [];

    /**
     * 项目列表
     *
     * @var array
     */
    public $apps = [];

    /**
     * 项目的计数器
     *
     * @var array
     */
    public $counterApp = [];

    public static $workerId = 0;

    protected static $DataJobs = [];

    protected static $shmKeys = [];

    public function __construct()
    {
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
     * @param $taskId
     * @param $uniqueId
     * @return DataJob
     */
    public function setBackup($taskId, $uniqueId)
    {
        if (isset(self::$DataJobs[$uniqueId]))
        {
            # 已经有存档的数据就不用处理了
            return;
        }

        if (!isset($this->jobs[$taskId][$uniqueId]))
        {
            self::$DataJobs[$uniqueId] = [$taskId, -1];
        }
        else
        {
            # 将对象克隆出来
            $obj = clone $this->jobs[$taskId][$uniqueId];
            self::$DataJobs[$uniqueId] = [$taskId, $obj];
        }
    }

    /**
     * 恢复
     */
    public function restore()
    {
        foreach (self::$DataJobs as $uniqueId => $item)
        {
            list($taskId, $obj) = $item;
            if ($obj === -1)
            {
                unset($this->jobs[$taskId][$uniqueId]);
            }
            else
            {
                $this->jobs[$taskId][$uniqueId] = $obj;
            }
        }

        # 清理数据
        $this->commit();
    }

    /**
     * 推送数据给task进程
     *
     * @return int
     */
    public function flush()
    {
        # 投递任务处理任务数据
        $time = microtime(1);

        if (SHMOP_MODE)
        {
            $rs = $this->flushByShm();
        }
        else
        {
            $rs = $this->flushByTask();
        }

        $this->delayCount = 0;
        foreach ($this->jobs as $taskId => $value)
        {
            foreach ($value as $uniqueId => $job)
            {
                /**
                 * @var $job DataJob
                 */
                if ($time < $job->taskTime)
                {
                    # 没有到投递时间
                    break;
                }

                $this->delayCount++;
            }
        }

        return $rs;
    }

    /**
     * 通过共享内存方式投递数据
     *
     * @return int
     */
    protected function flushByShm()
    {
        $count = 0;
        $i     = 0;
        $time  = microtime(1);

        # 之前投递失败的任务
        if (self::$shmKeys)foreach (self::$shmKeys as $uniqueId => $job)
        {
            if (EtServer::$server->task("shm|$uniqueId", $job))
            {
                unset(self::$shmKeys[$uniqueId]);
            }
        }

        # 所有任务ID列表
        $taskIds = array_keys($this->jobs);

        while($i < 10)
        {
            if (microtime(1) - $time > 3)break;

            foreach ($taskIds as $k => $taskId)
            {
                $shmKey = ($taskId * 100000) + (self::$workerId * 100) + $i;

                if ($shmId = shmop_open($shmKey, 'a', 0664, 0))
                {
                    # 还存在, 则表明任务进程还没有读取完毕
                    shmop_close($shmId);
                    continue;
                }

                $len  = 8;
                $keys = [];
                $str  = '';
                $j    = 0;
                $all  = true;

                foreach ($this->jobs[$taskId] as $uniqueId => $job)
                {
                    /**
                     * @var $job DataJob
                     */
                    if ($time < $job->taskTime)
                    {
                        # 没有到投递时间
                        break;
                    }

                    $j++;
                    $tmp    = msgpack_pack($job) . "\1\r\n";
                    $len   += strlen($tmp);
                    $str   .= $tmp;
                    $keys[] = $uniqueId;

                    unset($tmp);

                    if ($len > 10240000)
                    {
                        # 每10M发1次
                        $all = false;
                        break;
                    }
                }

                if ($j === 0)
                {
                    # 没有可读取的任务了
                    unset($taskIds[$k]);
                    $taskIds = array_values($taskIds);
                    continue;
                }

                # 获取一块内存
                $shmId = @shmop_open($shmKey, 'n', 0664, $len);
                if (false === $shmId)
                {
                    # 没创建成功, 可能共享内存已经用完
                    return $count;
                }

                # 前8位留给了记录成功的位置
                if (shmop_write($shmId, $str, 8))
                {
                    $count += $j;

                    foreach ($keys as $key)
                    {
                        unset($this->jobs[$taskId][$key]);
                    }

                    # 通知任务进程处理
                    if (!EtServer::$server->task("shm|$shmKey", $taskId))
                    {
                        # 没有投递成功则记录下, 下次再通知
                        self::$shmKeys[$shmKey] = $taskId;
                    }

                    if ($all)
                    {
                        # 移除列表
                        unset($taskIds[$k]);
                        $taskIds = array_values($taskIds);
                    }
                }
                else
                {
                    warn("shm write fail, data length: $len, shm id: $shmKey");
                }

                unset($str);
            }

            if (!$taskIds)break;

            $i++;
        }

        return $count;
    }

    /**
     * 通过任务投递方式投递数据
     *
     * @return int
     */
    protected function flushByTask()
    {
        $time    = microtime(1);
        $i       = 0;
        $count   = 0;
        # 所有任务ID列表
        $taskIds = array_keys($this->jobs);

        while($i < 100)
        {
            if (microtime(1) - $time > 3)break;

            foreach ($taskIds as $k => $taskId)
            {
                $j   = 0;
                $all = true;

                foreach ($this->jobs[$taskId] as $uniqueId => $job)
                {
                    /**
                     * @var $job DataJob
                     */
                    if ($time < $job->taskTime)
                    {
                        # 没有到投递时间
                        break;
                    }

                    if (EtServer::$server->task($job, $taskId))
                    {
                        # 投递成功移除对象
                        unset($this->jobs[$taskId][$uniqueId]);
                        $count++;
                    }
                    else
                    {
                        # 发送失败可能是缓冲区塞满了
                        $all = false;
                        warn("send task fail, task id: {$taskId}");
                        break;
                    }

                    $j++;
                    if ($j === 100)
                    {
                        $all = false;
                        break;
                    }
                }

                if ($all)
                {
                    unset($taskIds[$k]);
                    $taskIds = array_values($taskIds);
                }
            }

            if (!$taskIds)break;

            $i++;
        }

        return $count;
    }

    /**
     * 推送管理用的数据
     *
     * @param $redis
     */
    public function flushManagerData($redis)
    {
        if (!$redis)return;

        /**
         * @var Redis $redis
         */

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
}