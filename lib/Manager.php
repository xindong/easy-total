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
    public function onRequest(swoole_http_request $request, swoole_http_response $response)
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

        return true;
    }

    /**
     * webSocket协议收到消息
     *
     * @param swoole_server $server
     * @param swoole_websocket_frame $frame
     * @return mixed
     */
    public function onMessage(swoole_websocket_server $server, swoole_websocket_frame $frame)
    {
        # 给客户端发送消息
        # $server->push($frame->fd, 'data');
    }

    /**
     * webSocket端打开连接
     *
     * @param swoole_websocket_server $server
     * @param swoole_http_request $request
     * @return mixed
     */
    public function onOpen(swoole_websocket_server $server, swoole_http_request $request)
    {
        debug("server: handshake success with fd{$request->fd}");
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
            case 'task/update':
                # 添加一个任务

                if (!$this->worker->redis)
                {
                    $data['status']  = 'error';
                    $data['message'] = '请检查redis服务器';
                    goto send;
                }

                $sql = $this->request->post['sql'];
                if (!$sql)
                {
                    $data['status']  = 'error';
                    $data['message'] = '需要SQL参数';
                    goto send;
                }

                if ($option = SQL::parseSql($sql))
                {
                    if (isset($this->request->post['key']) && $this->request->post['key'])
                    {
                        # 指定任务key, 适用于更新任务
                        $old = $this->worker->redis->hGet('queries', $this->request->post['key']);
                        if ($old && $old = @unserialize($old))
                        {
                            $seriesKey = $old['seriesKey'];
                            if ($seriesKey !== $option['seriesKey'])
                            {
                                # 序列和原来的不一样, 表明修改过 where, group by, from 等条件, 需要重置序列
                                $oldSeries = $this->worker->redis->hGet('series', $seriesKey);
                                if ($oldSeries)$oldSeries = @unserialize($oldSeries);
                            }
                        }
                        $key = $option['key'] = $this->request->post['key'];
                    }
                    else
                    {
                        $key = $option['key'];
                    }

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
                        # 处理旧的序列设置
                        if (isset($oldSeries) && $oldSeries)
                        {
                            foreach ($oldSeries['queries'] as $k => $v)
                            {
                                if (false !== ($k2 = array_search($key, $v)))
                                {
                                    unset($oldSeries['queries'][$k][$k2]);

                                    if (!$oldSeries['queries'][$k])
                                    {
                                        unset($oldSeries['queries'][$k]);
                                    }
                                    else
                                    {
                                        $oldSeries['queries'][$k] = array_values($oldSeries['queries'][$k]);
                                    }
                                }
                            }

                            # 更新序列
                            $this->worker->redis->hSet('series', $oldSeries['key'], serialize($oldSeries));
                        }

                        # 通知所有worker进程更新SQL
                        $this->notifyAllWorker('task.reload');
                    }
                    else
                    {
                        $data['status']  = 'error';
                        $data['message'] = '更新服务器失败, 请检查redis服务器';
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
                    $data['message'] = '解析SQL失败';
                }

                break;

            case 'task/remove':
            case 'task/restore':
            case 'task/pause':
            case 'task/start':
                # 移除, 恢复, 暂停一个任务
                $option = null;
                if (isset($this->request->post['sql']))
                {
                    $sql    = $this->request->post['sql'];
                    $option = SQL::parseSql($sql);
                    if (!$option)
                    {
                        $data['status']  = 'error';
                        $data['message'] = '解析SQL失败';

                        goto send;
                    }

                    $sql = $option['sql'];
                    $key = null;
                    foreach ($this->worker->queries as $k => $query)
                    {
                        if ($sql === $query['sql'])
                        {
                            $key = $k;
                            break;
                        }
                    }

                    if (!$key)
                    {
                        $data['status']  = 'error';
                        $data['message'] = '找不到任务: '.$sql;
                        goto send;
                    }
                }
                elseif (isset($this->request->post['key']))
                {
                    $key = $this->request->post['key'];
                }
                elseif (isset($this->request->get['key']))
                {
                    $key = $this->request->get['key'];
                }
                else
                {
                    $data['status']  = 'error';
                    $data['message'] = '需要key或sql参数';
                    goto send;
                }

                if (isset($key))
                {
                    $option = $this->worker->redis->hGet('queries', $key);
                    if (!$option)
                    {
                        $data['status']  = 'error';
                        $data['message'] = "找不到key({$key})的任务";
                        goto send;
                    }
                    $option = @unserialize($option);

                    if (!$option)
                    {
                        $data['status']  = 'error';
                        $data['message'] = "数据解析错误 (key: {$key})";
                        goto send;
                    }
                }

                if ($option)
                {
                    switch ($uri)
                    {
                        case 'task/restore':
                            unset($option['deleteTime']);
                            break;

                        case 'task/pause':
                            # 标记为暂停
                            $option['use'] = false;
                            break;

                        case 'task/start':
                            # 标记为暂停
                            $option['use'] = true;
                            break;

                        default:
                            # 标记为移除
                            $option['deleteTime'] = time();
                            break;
                    }

                    $rs = false !== $this->worker->redis->hSet('queries', $key, serialize($option));
                }
                else
                {
                    $rs = false;
                }

                if ($rs)
                {
                    $data['status'] = 'ok';

                    # 通知所有进程
                    $this->notifyAllWorker('task.reload');

                    if (isset($sql))
                    {
                        if ($uri === 'task/restore')
                        {
                            info("restore sql: {$sql}");
                        }
                        elseif ($uri === 'task/pause')
                        {
                            info("restore sql: {$sql}");
                        }
                        elseif ($uri === 'task/start')
                        {
                            info("start sql: {$sql}");
                        }
                        else
                        {
                            info("remove sql: {$sql}");
                        }
                    }
                }
                else
                {
                    $data['status']  = 'error';
                    $data['msssage'] = '更新失败';
                }

                break;

            case 'task/list':
                try
                {
                    $rs      = [];
                    $queries = $this->worker->redis->hGetAll('queries');
                    if ($queries)foreach ($queries as $key => $query)
                    {
                        $query = @unserialize($query);
                        if ($query)
                        {
                            $rs[$query['key']] = $query['sql'];
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
            if (!$seriesOption['queries'][$groupKey] || (is_array($seriesOption['queries'][$groupKey]) && !in_array($option['key'], $seriesOption['queries'][$groupKey])))
            {
                $seriesOption['queries'][$groupKey][] = $option['key'];
            }
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