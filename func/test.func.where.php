<?php
/**
 * 这是一个测试的样例
 *
 * 最终需要返回一个可回调的方法, 并且本文件应该可以被重复 include, 实现动态加载的目的
 * 如果需要定义类, 尽量使用php7的匿名类
 *
 * 这样就可以在SQL里通过 where test(`field2`, `field2`, 1) < 123 这样调用
 */
return function($arg1, $arg2)
{
    return max($arg1, $arg2);
};