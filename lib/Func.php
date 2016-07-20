<?php

class SelectFunc
{
    /**
     * 合并的回调
     *
     * @var Closure
     */
    public $merge;

    /**
     * 加入新数据的回调
     *
     * @var Closure
     */
    public $push;

    /**
     * 获取数据的回调
     *
     * @var Closure
     */
    public $get;
}

class Func
{
    /**
     * 自定义列表
     *
     *
     * @var array
     */
    protected static $func = [];

    /**
     * 自定义 select 中的函数
     *
     * count|sum|max|min|avg|first|last|dist|exclude|value 不能被扩展
     *
     * @var array
     */
    protected static $funcSelect = [];

    /**
     * 加载自定义函数
     */
    public static function reload()
    {
        self::$func       = [];
        self::$funcSelect = [];

        foreach (glob(__FILE__.'/../func/*.func.where.php') as $item)
        {
            $name = strtolower(substr(basename($item), 0, -9));
            $rs = include $item;
            if (is_callable($rs))
            {
                self::$func[$name] = $rs;
            }
        }

        foreach (glob(__FILE__.'/../func/*.func.select.php') as $item)
        {
            $name = strtolower(substr(basename($item), 0, -9));
            $rs = include $item;
            if ($rs && is_object($rs) && $rs instanceof SelectFunc)
            {
                self::$funcSelect[$name] = $rs;
            }
        }
    }

    /**
     * 调用自定义的 select 中的函数
     *
     * @param $func
     * @param $field
     * @param $beforeData
     * @param $item
     * @param $itemTime
     * @return null|false
     */
    public static function callSelectMergeFun($func, $dataA, $dataB)
    {
        if (isset(self::$func[$func]))
        {
            $fun = self::$funcSelect[$func]->merge;
            if (!$fun)return false;

            return $fun($dataA, $dataB);
        }
        else
        {
            return null;
        }
    }

    /**
     * 调用自定义的 select 中的函数
     *
     * @param $func
     * @param $field
     * @param $beforeData
     * @param $item
     * @param $itemTime
     * @return null|false
     */
    public static function callSelectPushFun($func, $field, $beforeData, $item, $itemTime)
    {
        if (isset(self::$func[$func]))
        {
            $fun = self::$funcSelect[$func]->push;
            if (!$fun)return false;

            return $fun($field, $beforeData, $item, $itemTime);
        }
        else
        {
            return null;
        }
    }

    /**
     * 调用自定义的 select 中的函数
     *
     * @param $func
     * @param $field
     * @param $beforeData
     * @param $item
     * @param $itemTime
     * @return null
     */
    public static function callSelectGetDataFun($func, $field, $total, DataJob $job)
    {
        if (isset(self::$func[$func]))
        {
            $fun = self::$funcSelect[$func]->get;
            if (!$fun)return null;

            return $fun($field, $total, $job);
        }
        else
        {
            return null;
        }
    }

    /**
     * 执行自定义的 where 中的函数
     *
     * @param $fun
     * @param $arg
     * @return bool|mixed|null
     */
    public static function callWhereFun($func, $arg)
    {
        if (isset(self::$func[$func]))
        {
            $fun = self::$func[$func];

            try
            {
                switch (count($arg))
                {
                    case 0:
                        $value = $fun();
                        break;
                    case 1:
                        $value = $fun($arg[0]);
                        break;
                    case 2:
                        $value = $fun($arg[0], $arg[1]);
                        break;
                    case 3:
                        $value = $fun($arg[0], $arg[1], $arg[2]);
                        break;
                    case 4:
                        $value = $fun($arg[0], $arg[1], $arg[2], $arg[3]);
                        break;
                    default:
                        $value = @call_user_func_array($fun, $arg);
                        break;
                }
            }
            catch (Exception $e)
            {
                $value = false;
            }

            return $value;
        }
        else
        {
            return null;
        }
    }
}