<?php

if (class_exists('Thread', false))
{
    /**
     * 支持多线程接口的对象
     */
    class DataObject extends Thread {}
}
else
{
    /**
     * 支持数组对象的对象
     */
    class DataObject extends ArrayObject {}
}

/**
 * 任务数据对象
 */
class DataJobs extends DataObject {}


/**
 * 唯一数列对象
 */
class DataDist extends DataObject {}


/**
 * 统计数据对象
 */
class DataTotal extends DataObject {}


/**
 * 统计数据对象
 */
class DataTotalItem
{
    public $lastLoadTime = 0;

    /**
     * @var DataObject
     */
    public $count;

    /**
     * @var DataObject
     */
    public $dist;

    /**
     * @var DataObject
     */
    public $sum;

    /**
     * @var DataObject
     */
    public $min;

    /**
     * @var DataObject
     */
    public $max;

    /**
     * @var DataObject
     */
    public $first;

    /**
     * @var DataObject
     */
    public $last;

    /**
     * 数据类型
     *
     * @var array
     */
    private static $allType = ['dist', 'count', 'sum', 'min', 'max', 'first', 'last'];

    public function __construct()
    {
        foreach (self::$allType as $item)
        {
            if (!$this->$item)
            {
                $this->$item = new DataObject();
            }
        }
    }

    public function __sleep()
    {
        $rs = [];
        foreach (self::$allType as $item)
        {
            if ($this->$item->count())
            {
                $rs[] = $item;
            }
        }

        return $rs;
    }

    public function __wakeup()
    {
        # 调用初始化对象
        $this->__construct();
    }
}
