<?php
/**
 * 这是一个测试的样例
 *
 * 不同于 where 的回调函数, select 函数的参数是固定的
 *
 * count|sum|max|min|avg|first|last|dist|exclude|value 为系统自动功能, 不能被扩展
 *
 * 最终需要返回一个可回调的方法, 并且本文件应该可以被重复 include, 实现动态加载的目的
 * 如果需要定义类, 尽量使用php7的匿名类
 *
 * 这样就可以在SQL里通过 select test(`field1`) 调用
 *
 */

$obj = new SelectFunc();

/**
 * 这个对象是用来添加数据的回调
 *
 * @param string $field      当前的字段名
 * @param mixed  $beforeData 之前这个函数返回的数据
 * @param array  $item       新数据的数组
 * @param int    $itemTime   新数据的时间
 */
$obj->push = function($field, $beforeData, $item, $itemTime)
{
    # 这里演示获取一个最大值

    return max($beforeData, $item[$field]);
};

/**
 * 这个对象是用来合并2个数据的回调
 *
 * $data1, $data1 均为之前通过 push 或 merge 返回的值
 *
 * @param mixed $data1
 * @param mixed $data2
 */
$obj->merge = function($data1, $data2)
{
    # 这里演示获取一个最大值

    return max($data1, $data2);
};

/**
 * 这个对象是用来获取最终数据的回调
 *
 * @param string  $field 当前字段名
 * @param mixed   $data  通过 merge 或 push 回调的最终数据
 * @param DataJob $job   当前任务数据的对象
 */
$obj->merge = function($field, $data, DataJob $job)
{
    # 这里演示获取一个最大值, 直接返回之前统计处理的数值

    return intval($data);
};

return $obj;
