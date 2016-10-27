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
