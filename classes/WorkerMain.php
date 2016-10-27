<?php

class WorkerMain extends MyQEE\Server\WorkerHttp
{
    /**
     * @var redis
     */
    public $redis;

    /**
     * SimpleSSDB 对象
     *
     * @var SimpleSSDB
     */
    public $ssdb;

    public function onStart()
    {
        if ($this->id == 0)
        {
            # 计数器只支持42亿的计数, 所以每小时检查计数器是否快溢出
            swoole_timer_tick(1000 * 60 * 60, function()
            {
                if (($count = EtServer::$counter->get()) > 100000000)
                {
                    # 将1亿的余数记录下来
                    EtServer::$counter->set($count % 100000000);
                    EtServer::$counterX->add(intval($count / 100000000));
                }
            });
        }
    }
}