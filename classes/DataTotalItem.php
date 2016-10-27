<?php

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
     * @var array
     */
    public $func = [];

    /**
     * 是否从db处加载的
     *
     * @var bool
     */
    public $all = false;

    public function __sleep()
    {
        $rs = [];
        foreach (['dist', 'count', 'sum', 'min', 'max', 'first', 'last', 'func'] as $item)
        {
            if ($this->$item)
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
