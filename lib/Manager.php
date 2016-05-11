<?php

class Manager
{
    /**
     * @var swoole_http_server
     */
    protected $server;

    /**
     * @var Worker
     */
    protected $worker;

    /**
     * @var int
     */
    protected $workerId = 0;

    /**
     * @var swoole_http_request
     */
    protected $request;

    /**
     * @var swoole_http_response
     */
    protected $response;

    /**
     * Manager constructor.
     */
    public function __construct($server, $worker, $workerId)
    {
        $this->server   = $server;
        $this->worker   = $worker;
        $this->workerId = $workerId;

        require_once __DIR__ .'/SQL.php';
    }

    /**
     * @param swoole_http_request $request
     * @param swoole_http_response $response
     * @return mixed
     */
    public function onManagerRequest(swoole_http_request $request, swoole_http_response $response)
    {
        $this->request  = $request;
        $this->response = $response;

        $uri    = trim($request->server['request_uri'], ' /');
        $uriArr = explode('/', $uri);
        $type   = array_shift($uriArr);

        switch ($type)
        {
            case 'api':
                $this->api(implode('/', $uriArr));
                break;

            case 'admin':
                $this->admin(implode('/', $uriArr));
                break;

            case 'assets':
                $this->assets(implode('/', $uriArr));
                break;

            default:

                $response->status(404);
                $response->end('page not found');
                break;
        }

        $this->request  = null;
        $this->response = null;
    }

    protected function admin($uri)
    {
        if ($uri === '')
        {
            $uri = 'index';
        }
        else
        {
            $uri  = str_replace(['\\', '../'], ['/', '/'], $uri);
        }

        $file = __DIR__ .'/../admin/'. $uri .'.php';
        debug($file);

        if (!is_file($file))
        {
            $this->response->status(404);
            $this->response->end('page not found');
            return;
        }

        ob_start();
        include __DIR__ .'/../admin/_header.php';
        $rs = include $file;
        if ($rs !== 'noFooter')
        {
            include __DIR__ . '/../admin/_footer.php';
        }
        $html = ob_get_clean();

        $this->response->end($html);
    }

    protected function api($uri)
    {
        $data = [];

        switch ($uri)
        {
            case 'task/add':
                # 添加一个任务

                if (!$this->worker->redis)
                {
                    $data['status']  = 'error';
                    $data['message'] = 'redis server is not active';
                    goto send;
                }

                $sql = $this->request->post['sql'];
                if (!$sql)
                {
                    $data['status']  = 'error';
                    $data['message'] = 'need parameter sql';
                    goto send;
                }

                if ($option = SQL::parseSql($sql))
                {
                    $key = $option['key'];

                    if ($this->request->post['name'])
                    {
                        $option['name'] = trim($this->request->post['name']);
                    }

                    if ($this->request->post['start'] > time())
                    {
                        # 开始时间
                        $option['start'] = (int)$this->request->post['start'];
                    }

                    if ($this->request->post['end'] > time())
                    {
                        # 结束时间
                        $option['end'] = (int)$this->request->post['start'];
                    }

                    if ($this->createSeriesByQueryOption($option) && false !== $this->worker->redis->hSet('queries', $key, serialize($option)))
                    {
                        # 通知所有worker进程更新SQL
                        $this->notifyAllWorker('task.reload');
                    }
                    else
                    {
                        $data['status']  = 'error';
                        $data['message'] = 'update setting error, please check redis server.';
                        goto send;
                    }

                    if (IS_DEBUG)
                    {
                        echo "new option: ";
                        print_r($option);
                    }

                    $data['status'] = 'ok';
                    $data['key']    = $key;
                    $data['sql']    = $option['sql'];

                    info("fork new sql($key): {$data['sql']}");
                }
                else
                {
                    $data['status']  = 'error';
                    $data['message'] = 'parse sql error';
                }

                break;

            case 'task/remove':
                # 移除一个任务
                $option = null;
                if (isset($this->request->post['sql']))
                {
                    $sql    = $this->request->post['sql'];
                    $option = SQL::parseSql($sql);
                    if (!$option)
                    {
                        $data['status']  = 'error';
                        $data['message'] = 'parse sql error';

                        goto send;
                    }

                    $key   = $option['key'];
                    $save  = key($option['saveAs']);
                }
                elseif (isset($this->request->post['key']) && isset($this->request->post['save']))
                {
                    $key  = $this->request->post['key'];
                    $save = $this->request->post['save'];
                }
                elseif (isset($this->request->get['key']) && isset($this->request->get['save']))
                {
                    $key  = $this->request->get['key'];
                    $save = $this->request->get['save'];
                }
                else
                {
                    $data['status']  = 'error';
                    $data['message'] = 'need arguments [key and table] or [sql]';
                    goto send;
                }

                if (isset($key) && isset($save))
                {
                    $option = $this->worker->redis->hGet('queries', $key);
                    if (!$option)
                    {
                        $data['status']  = 'error';
                        $data['message'] = "can not found (key={$key},saveAs={$save}) task";
                        goto send;
                    }
                    $option = @unserialize($option);

                    if (!$option)
                    {
                        $data['status']  = 'error';
                        $data['message'] = "数据解析错误,无法删除 (key={$key},saveAs={$save}) task";
                        goto send;
                    }
                }

                $sendType = 'task.update';
                if ($option)
                {
                    $sql = $option['sql'][$save];
                    unset($option['saveAs'][$save]);

                    if (!$option['saveAs'])
                    {
                        # 没有其它任务, 可直接清除
                        $rs       = $this->worker->redis->hDel('queries', $key);
                        $sendType = 'task.remove';
                    }
                    else
                    {
                        unset($option['sql'][$save]);

                        # 更新function
                        $option['function'] = [];
                        $allField           = false;
                        foreach ($option['saveAs'] as $opt)
                        {
                            if ($opt['allField'])$allField = true;
                            foreach ($opt['field'] as $st)
                            {
                                $type  = $st['type'];
                                $field = $st['field'];
                                switch ($type)
                                {
                                    case 'avg':
                                        $option['function']['sum'][$field]  = true;
                                        $option['function']['count']['*']   = true;
                                        break;

                                    case 'dist':
                                    case 'list':
                                    case 'listcount':
                                        $option['function']['dist'][$field] = true;
                                        break;

                                    case 'count':
                                        $option['function']['count']['*'] = true;
                                        break;

                                    default:
                                        $option['function'][$type][$field] = true;
                                        break;
                                }
                            }
                        }
                        # 是否全部字段
                        $option['allField'] = $allField;

                        # 更新数据
                        $rs = $this->worker->redis->hSet('queries', $key, serialize($option));
                    }
                }
                else
                {
                    $rs = false;
                }

                if ($rs)
                {
                    $data['status'] = 'ok';

                    # 通知所有进程清理数据
                    for ($i = 0; $i < $this->server->setting['worker_num']; $i++)
                    {
                        # 通知更新
                        $msg = [
                            'type' => $sendType,
                            'key'  => $key,
                        ];

                        if ($i == $this->workerId)
                        {
                            # 直接请求
                            $this->worker->onPipeMessage($this->server, $this->workerId, json_encode($msg));
                        }
                        else
                        {
                            # 通知执行
                            $this->server->sendMessage(json_encode($msg), $i);
                        }
                    }

                    if ($sendType === 'task.remove')
                    {
                        # 异步清理redis中的数据
                        # todo 异步删除数据
                        $this->server->task("clearByKey|{$key}|{$option['table']}|{$save}");
                    }

                    if (isset($sql))
                    {
                        info("remove sql: {$sql}");
                    }
                }
                else
                {
                    $data['status']  = 'error';
                }

                break;

            case 'task/list':
                try
                {
                    $rs      = [];
                    $queries = $this->worker->redis->hGetAll('queries');
                    if ($queries)foreach ($queries as $key => $query)
                    {
                        $query = unserialize($query);
                        foreach ($query['sql'] as $table => $sql)
                        {
                            $rs[] = $sql;
                        }
                    }

                    $data['sql'] = $rs;
                }
                catch (Exception $e)
                {
                    $data['status']  = 'error';
                    $data['message'] = 'please check redis server.';
                }

                break;

            case 'task/pause':
                # 暂停任务
                if (isset($this->request->get['key']))
                {
                    $key = $this->request->get['key'];
                }
                else
                {
                    $data['status']  = 'error';
                    $data['message'] = 'need parameter key';
                    break;
                }

                break;

            case 'task/start':
                # 开启任务任务
                if (isset($this->request->get['key']))
                {
                    $key = $this->request->get['key'];
                }
                else
                {
                    $data['status']  = 'error';
                    $data['message'] = 'need parameter key';
                    break;
                }

                break;

            case 'server/stats':
                $data['status'] = 'ok';
                $data['data']   = $this->server->stats();
                break;

            case 'server/restart':
            case 'server/reload':
                # 重启所有进程
                $data['status'] = 'ok';

                debug('restart server by api from ip: '. $this->request->server['remote_addr']);

                # 200 毫秒后重启
                swoole_timer_after(200, function()
                {
                    $this->server->reload();
                });

                break;

            default:
                $data['status']  = 'error';
                $data['message'] = 'unknown action: ' . $uri;
                break;
        }

        send:
        $this->response->header('Content-Type', 'application/json');
        $this->response->end(json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE)."\n");

        return null;
    }

    /**
     * 输出静态文件
     *
     * @param $uri
     */
    protected function assets($uri)
    {
        $uri  = str_replace(['\\', '../'], ['/', '/'], $uri);
        $rPos = strrpos($uri, '.');
        if (false === $rPos)
        {
            # 没有任何后缀
            $this->response->status(404);
            $this->response->end('page not found');
            return;
        }

        $type = strtolower(substr($uri, $rPos + 1));

        $header = [
            'js'    => 'application/x-javascript',
            'css'   => 'text/css',
            'png'   => 'image/png',
            'jpg'   => 'image/jpeg',
            'jpeg'  => 'image/jpeg',
            'gif'   => 'image/gif',
            'json'  => 'application/json',
            'svg'   => 'image/svg+xml',
            'woff'  => 'application/font-woff',
            'woff2' => 'application/font-woff2',
            'ttf'   => 'application/x-font-ttf',
            'eot'   => 'application/vnd.ms-fontobject',
        ];

        if (isset($header[$type]))
        {
            $this->response->header('Content-Type', $header[$type]);
        }

        $file = __DIR__ .'/../assets/'. $uri;
        if (is_file($file))
        {
            # 设置缓存头信息
            $time = 86400;
            $this->response->header('Cache-Control', 'max-age='. $time);
            $this->response->header('Last-Modified', date('D, d M Y H:i:s \G\M\T', filemtime($file)));
            $this->response->header('Expires'      , date('D, d M Y H:i:s \G\M\T', time() + $time));
            $this->response->header('Pragma'       , 'cache');

            $this->response->end(file_get_contents($file));
        }
        else
        {
            $this->response->status(404);
            $this->response->end('assets not found');
        }
    }

    /**
     * 创建一个序列设置, 如果存在则合并
     *
     * @param $option
     * @return bool
     */
    protected function createSeriesByQueryOption($option)
    {
        $seriesKey = $option['seriesKey'];

        if (isset($this->worker->series[$seriesKey]))
        {
            # 已经存在, where 和 group by 是一样的所以不用合并了
            $seriesOption = $this->worker->series[$seriesKey];
            $seriesOption['function']  = array_merge_recursive($seriesOption['function'], $option['function']);
            $seriesOption['groupTime'] = array_merge($seriesOption['groupTime'], $option['groupTime']);

            if ($option['for'])
            {
                $seriesOption['for'] = array_merge($seriesOption['for'], $option['for']);
            }
            else
            {
                $option['allApp'] = true;
            }

            # 开始时间
            if ($seriesOption['start'] < time() || $option['start'] < time())
            {
                $seriesOption['start'] = 0;
            }
            else
            {
                $seriesOption['start'] = min($option['start'], $seriesOption['start']);
            }

            # 结束时间
            if ($seriesOption['end'] == 0 || $option['end'] == 0 || $seriesOption['end'] > time() || $option['end'] > time())
            {
                $seriesOption['end'] = 0;
            }
            else
            {
                $seriesOption['end'] = max($option['end'], $seriesOption['end']);
            }
        }
        else
        {
            $seriesOption = [
                'key'       => $option['seriesKey'],
                'use'       => $option['use'],
                'start'     => $option['start'],
                'end'       => $option['end'],
                'allApp'    => $option['for'] ? false : true,
                'for'       => $option['for'],
                'table'     => $option['table'],
                'where'     => $option['where'],
                'groupBy'   => $option['groupBy'],
                'groupTime' => $option['groupTime'],
                'function'  => $option['function'],
                'queries'   => [],
            ];
        }

        # 设置查询的映射
        foreach ($option['groupTime'] as $groupKey => $st)
        {
            $seriesOption['queries'][$groupKey][] = $option['key'];
            $seriesOption['queries'][$groupKey]   = array_unique($seriesOption['queries'][$groupKey]);
        }

        return false !== $this->worker->redis->hSet('series', $seriesKey, serialize($seriesOption));
    }

    protected function notifyAllWorker($data)
    {
        for ($i = 0; $i < $this->server->setting['worker_num']; $i++)
        {
            # 每个服务器通知更新
            if ($i == $this->workerId)
            {
                $this->worker->onPipeMessage($this->server, $this->workerId, $data);
            }
            else
            {
                $this->server->sendMessage($data, $i);
            }
        }
    }
}