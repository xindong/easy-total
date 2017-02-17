<?php

class WorkerMain extends MyQEE\Server\WorkerHttp
{
    /**
     * @var WorkerManager
     */
    protected $manager;

    /**
     * @var WorkerAPI
     */
    protected $api;

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

        # 管理进程
        $manger        = new WorkerManager($this->server, 'Manager', $this->id);
        $this->manager = $manger;
        \MyQEE\Server\Server::$workers[$manger->name] = $manger;

        # API进程
        $api       = new WorkerAPI($this->server, 'Manager', $this->id);
        $api->name = 'Manager';
        $this->api = $api;
        \MyQEE\Server\Server::$workers[$api->name] = $api;

        $manger->onStart();
        $api->onStart();
    }

    /**
     * @param \Swoole\Http\Request $request
     * @param \Swoole\Http\Response $response
     */
    public function onRequest($request, $response)
    {
        if ($this->api->isApi($request))
        {
            # API
            $this->api->onRequest($request, $response);
        }
        elseif ($this->manager->isManager($request))
        {
            # ADMIN
            $this->manager->onRequest($request, $response);
        }
        else
        {
            parent::onRequest($request, $response);
        }
    }
}