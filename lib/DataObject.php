<?php

/**
 * 任务数据对象
 */
class DataJob
{
    /**
     * 统计系统分配的唯一ID
     *
     * Exp: abcde123af32,1d,hsqj,2016001,123_abc
     *
     * @var string
     */
    public $uniqueId;

    /**
     * 数据的唯一ID
     *
     * @var string
     */
    public $dataId;

    /**
     * 时间分组值
     *
     * @var int
     */
    public $timeOpLimit;

    /**
     * 时间的分组类型
     *
     * m,d,h,i,s
     *
     * @var string
     */
    public $timeOpType;

    /**
     * 时间分组的key
     *
     * @var string
     */
    public $timeKey;

    /**
     * 当前数据的时间戳
     *
     * @var int
     */
    public $time;

    /**
     * 当前数据对应的应用
     *
     * @var string
     */
    public $app;

    /**
     * 当前数据对应的序列的key
     *
     * @var string
     */
    public $seriesKey;

    /**
     * 当前数据的唯一数据列表
     *
     * @var array
     */
    public $dist = [];

    /**
     * 相关统计的数据
     *
     * @var DataTotalItem
     */
    public $total;

    /**
     * 数据内容
     *
     * @var array
     */
    public $data = [];

    /**
     * 分配的任务投递时间
     *
     * @var int
     */
    public $taskTime = 0;

    /**
     * 活跃时间
     *
     * @var int
     */
    public $activeTime = 0;

    /**
     * 是否已保存
     *
     * @var bool
     */
    public $saved = false;

    /**
     * 序列化后的字符
     *
     * @var string
     */
    public $_serialized;

    public function __construct($uniqueId)
    {
        $this->uniqueId = $uniqueId;
        $this->total    = new DataTotalItem();
    }

    public function __sleep()
    {
        $array = [
            'uniqueId'    => $this->uniqueId,
            'dataId'      => $this->dataId,
            'timeOpLimit' => $this->timeOpLimit,
            'timeOpType'  => $this->timeOpType,
            'timeKey'     => $this->timeKey,
            'time'        => $this->time,
            'app'         => $this->app,
            'seriesKey'   => $this->seriesKey,
            'taskTime'    => $this->taskTime,
            'dist'        => [],
        ];
        # 导出唯一列表
        foreach ($this->dist as $field => $v)
        {
            $array['dist'][$field] = array_keys($v);
        }

        $this->_serialized = json_encode($array, JSON_UNESCAPED_UNICODE);
        return ['_serialized', 'data', 'total'];
    }

    public function __wakeup()
    {
        if ($this->_serialized)
        {
            $data = @json_decode($this->_serialized, true);
            if ($data)
            {
                foreach ($data as $key => $value)
                {
                    if ($key !== 'dist')
                    {
                        $this->$key = $value;
                    }
                }

                # 恢复唯一列表
                foreach ($data['dist'] as $field => $value)
                {
                    foreach ($value as $item)
                    {
                        $this->dist[$field][$item] = 1;
                    }
                }
            }

            $this->_serialized = null;

            if (!$this->total)
            {
                $this->total = new DataTotalItem();
            }
        }
    }

    public function setData($item, $fun, $allField)
    {
        # 记录唯一值
        if (isset($fun['dist']))
        {
            foreach ($fun['dist'] as $field => $t)
            {
                if (true === $t)
                {
                    # 单字段
                    $v = $item[$field];
                }
                else
                {
                    # 多字段
                    $v = [];
                    foreach ($t as $f)
                    {
                        $v[] = $item[$f];
                    }
                    $v = implode('_', $v);
                }

                $this->dist[$field][$v] = 1;
            }
        }

        $time = isset($item['microtime']) && $item['microtime'] > $item['time'] && $item['microtime'] - $this->time < 1 ? $item['microtime'] : $this->time;
        self::totalData($this->total, $item, $fun, $time);

        if ($allField)
        {
            # 需要所有字段数据
            $data = $item;
        }
        else
        {
            $data = [];
            if (isset($option['function']['value']))
            {
                # 所有需要赋值的字段, 不需要的字段全部丢弃
                foreach ($option['function']['value'] as $field => $tmp)
                {
                    if (isset($item[$field]))
                    {
                        $data[$field] = $item[$field];
                    }
                }
            }
        }

        $this->data = $data;
    }

    /**
     * 将一个新的job数据合并进来
     *
     * @param DataJob $job
     * @return bool
     */
    public function merge(DataJob $job)
    {
        if ($job->uniqueId !== $this->uniqueId)return false;

        # 合并唯一序列
        foreach ($job->dist as $field => $v)
        {
            if (isset($this->dist[$field]))
            {
                $this->dist[$field] = array_merge($this->dist[$field], $v);
            }
            else
            {
                $this->dist[$field] = $v;
            }
        }

        # 合并统计数据
        if ($job->total)
        {
            $this->mergeTotal($job->total);
        }

        $this->dataId = $job->dataId;
        $this->time   = $job->time;
        $this->data   = $job->data;

        return true;
    }


    /**
     * 合并统计数值
     *
     * @param DataTotalItem $newTotal
     */
    public function mergeTotal(DataTotalItem $newTotal)
    {
        # 相加的数值
        foreach ($newTotal->sum as $field => $v)
        {
            $this->total->sum[$field] += $v;
        }

        foreach ($newTotal->count as $field => $v)
        {
            $this->total->count[$field] += $v;
        }

        foreach ($newTotal->last as $field => $v)
        {
            $tmp = $this->total->last[$field];

            if (!$tmp || $tmp[1] < $v[1])
            {
                $this->total->last[$field] = $v;
            }
        }

        foreach ($newTotal->first as $field => $v)
        {
            $tmp = $this->total->first[$field];

            if (!$tmp || $tmp[1] > $v[1])
            {
                $this->total->first[$field] = $v;
            }
        }

        foreach ($newTotal->min as $field => $v)
        {
            if (isset($this->total->min[$field]))
            {
                $this->total->min[$field] = min($v, $this->total->min[$field]);
            }
            else
            {
                $this->total->min[$field] = $v;
            }
        }

        foreach ($newTotal->max as $field => $v)
        {
            if (isset($this->total->max[$field]))
            {
                $this->total->max[$field] = max($v, $this->total->max[$field]);
            }
            else
            {
                $this->total->max[$field] = $v;
            }
        }

        foreach ($newTotal->dist as $field => $v)
        {
            $this->total->dist[$field] = max($this->total->dist[$field], $v);
        }

        # 更新时间
        if ($newTotal->all)
        {
            $this->total->all = true;
        }
    }

    /**
     * 获取当前任务的投递ID
     *
     * @return int
     */
    public function taskId()
    {
        return self::getTaskId($this->seriesKey, $this->timeOpType === '-' ? '-' : $this->timeOpLimit.$this->timeOpType, $this->app);
    }


    /**
     * 根据任务key获取taskId
     *
     * 不分配id = 0的任务
     *
     * @param $seriesKey
     * @param $timeOptKey
     * @param $app
     * @return int
     */
    public static function getTaskId($seriesKey, $timeOptKey, $app)
    {
        $taskKey      = "$seriesKey,$timeOptKey,$app";
        static $cache = [];
        if (isset($cache[$taskKey]))return $cache[$taskKey];

        $taskNum = EtServer::$server->setting['task_worker_num'] - 1;

        if (count($cache) > 200)
        {
            $cache = array_slice($cache, -20, null, true);
        }

        $cache[$taskKey] = (crc32(md5($taskKey)) % ($taskNum - 1)) + 1;

        return $cache[$taskKey];
    }

    /**
     * 获取任务延时处理的时间规则
     *
     * @param $set
     * @return int
     */
    public static function getDelayTime($set)
    {
        if (true === $set)return 60;

        switch ($set[1])
        {
            case 'M':      // 分钟
            case 'i':      // 分钟
                if ($set[0] < 10)
                {
                    return 60;
                }
                else
                {
                    return 600;
                }

            case 's':      // 秒
            case '-':      // 不分组
                return 60;

            default:
                # 其它的保存间隔为10分钟
                return 600;
        }
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
    protected static function totalData(DataTotalItem $total, $item, $fun, $time)
    {
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
                    $total->min[$field] = min($total->min[$field], $item[$field]);
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
                    $total->max[$field] = max($total->max[$field], $item[$field]);
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



/**
 * 统计数据对象
 */
class DataTotalItem
{
    /**
     * @var array
     */
    public $count = [];

    /**
     * @var array
     */
    public $dist = [];

    /**
     * @var array
     */
    public $sum = [];

    /**
     * @var array
     */
    public $min = [];

    /**
     * @var array
     */
    public $max = [];

    /**
     * @var array
     */
    public $first = [];

    /**
     * @var array
     */
    public $last = [];

    /**
     * 是否从db处加载的
     *
     * @var bool
     */
    public $all = false;

    public function __sleep()
    {
        $rs = [];
        foreach (['dist', 'count', 'sum', 'min', 'max', 'first', 'last'] as $item)
        {
            if (count($this->$item))
            {
                $rs[] = $item;
            }
        }
        $rs[] = 'all';

        return $rs;
    }

    public function __wakeup()
    {

    }
}
