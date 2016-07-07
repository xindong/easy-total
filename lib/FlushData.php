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

    public static $workerId = 0;

    protected static $DataJobs = [];

    protected static $shmKeys = [];

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
     * 获取一个任务对象
     *
     * @param $taskId
     * @param $uniqueId
     * @return DataJob
     */
    public function getJob($taskId, $uniqueId)
    {
        if (!isset($this->jobs[$taskId][$uniqueId]))
        {
            $this->jobs[$taskId][$uniqueId] = new DataJob($uniqueId);
        }

        return $this->jobs[$taskId][$uniqueId];
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

    public function flush($redis)
    {
        /**
         * @var Redis $redis
         */

        # 投递任务处理任务数据
        if ($this->jobs)
        {
            if (SHMOP_MODE)
            {
                $this->flushByShm();
            }
            else
            {
                $this->flushByTask();
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
     * 通过共享内存方式投递数据
     */
    protected function flushByShm()
    {
        if (self::$shmKeys)foreach (self::$shmKeys as $k => $v)
        {
            if (EtServer::$server->task("shm|$k", $v))
            {
                unset(self::$shmKeys[$k]);
            }
        }

        $i    = 0;
        $time = microtime(1);
        $err  = [];
        while($i < 100)
        {
            if (microtime(1) - $time > 3)break;

            foreach ($this->jobs as $taskId => $value)
            {
                if (isset($err[$taskId]))
                {
                    continue;
                }

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
                $all  = true;
                $j    = 0;
                foreach ($value as $k => $v)
                {
                    $j++;
                    $tmp    = msgpack_pack($v) . "\r\n";
                    $len   += strlen($tmp);
                    $keys[] = $k;
                    $str   .= $tmp;

                    unset($tmp);

                    if ($len > 20480000 || $j === 100)
                    {
                        $all = false;
                        break;
                    }
                }

                # 获取一块内存
                $shmId = @shmop_open($shmKey, 'n', 0664, $len);
                if (false === $shmId)
                {
                    # 没创建成功
                    continue;
                }

                # 前8位留给了记录成功的位置
                if (shmop_write($shmId, $str, 8))
                {
                    if ($all)
                    {
                        # 全部记录成功
                        unset($this->jobs[$taskId]);
                    }
                    else
                    {
                        # 部分记录成功
                        foreach ($keys as $k)
                        {
                            unset($this->jobs[$taskId][$k]);
                        }
                    }

                    # 通知任务进程处理
                    if (!EtServer::$server->task("shm|$shmKey", $taskId))
                    {
                        # 没有投递成功则记录下, 下次再通知
                        self::$shmKeys[$shmKey] = $taskId;
                    }
                }
                else
                {
                    warn("shm write fail, data length: $len, shm id: $shmKey");
                }

                unset($str);

                if (!$this->jobs[$taskId])
                {
                    unset($this->jobs[$taskId]);
                }
            }

            if (!$this->jobs)break;

            $i++;
        }
    }

    /**
     * 通过任务投递方式投递数据
     */
    protected function flushByTask()
    {
        $i    = 0;
        $time = microtime(1);
        while($i < 100)
        {
            if (microtime(1) - $time > 3)break;

            foreach ($this->jobs as $taskId => $value)
            {
                # 投递数据, 每次执行不超过3秒钟
                $j = 0;
                foreach ($value as $k => $v)
                {
                    if (EtServer::$server->task($v, $taskId))
                    {
                        # 投递成功移除对象
                        unset($this->jobs[$taskId][$k]);
                    }
                    else
                    {
                        # 发送失败可能是缓冲区塞满了
                        break;
                    }

                    $j++;
                    if ($j === 100)
                    {
                        break;
                    }
                }

                if (!$this->jobs[$taskId])
                {
                    unset($this->jobs[$taskId]);
                }
            }

            if (!$this->jobs)break;

            $i++;
        }
    }
}