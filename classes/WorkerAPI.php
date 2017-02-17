<?php

class WorkerAPI extends MyQEE\Server\WorkerAPI
{
    /**
     * @var WorkerEasyTotal
     */
    public $worker;

    public function onStart()
    {
        $this->worker = EtServer::$workers['EasyTotal'];
    }

    /**
     * @param \Swoole\Http\Request $request
     * @param \Swoole\Http\Response $response
     */
    public function onRequest($request, $response)
    {
        $uri    = trim($request->server['request_uri'], ' /');
        $uriArr = explode('/', $uri);
        array_shift($uriArr);

        $uri  = implode('/', $uriArr);
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

                $sql = $request->post['sql'];
                if (!$sql)
                {
                    $data['status']  = 'error';
                    $data['message'] = '需要SQL参数';
                    goto send;
                }

                if ($option = SQL::parseSql($sql))
                {
                    if (isset($request->post['key']) && $request->post['key'])
                    {
                        # 指定任务key, 适用于更新任务
                        $old = $this->worker->redis->hGet('queries', $request->post['key']);
                        if ($old && $old = @unserialize($old))
                        {
                            $seriesKey = $old['seriesKey'];
                            if ($seriesKey !== $option['seriesKey'])
                            {
                                # 序列和原来的不一样, 表明修改过 where, group by, from 等条件, 需要重置序列
                                $oldSeries = $this->worker->redis->hGet('series', $seriesKey);
                                if ($oldSeries)$oldSeries = @unserialize($oldSeries);
                            }

                            # 使用老的参数
                            $option['use']        = $old['use'];
                            $option['createTime'] = $old['createTime'];
                            $option['editTime']   = time();
                        }
                        $key = $option['key'] = $request->post['key'];
                    }
                    else
                    {
                        $old = [];
                        $key = $option['key'];
                    }

                    if ($request->post['name'])
                    {
                        $option['name'] = trim($request->post['name']);
                    }
                    elseif ($old['name'])
                    {
                        $option['name'] = $old['name'];
                    }

                    if ($request->post['start'] > time())
                    {
                        # 开始时间
                        $option['start'] = (int)$request->post['start'];
                    }
                    elseif ($old['start'])
                    {
                        $option['start'] = $old['start'];
                    }

                    if ($request->post['end'] > time())
                    {
                        # 结束时间
                        $option['end'] = (int)$request->post['start'];
                    }
                    elseif ($old['end'])
                    {
                        $option['end'] = $old['end'];
                    }

                    $seriesOption = self::createSeriesByQueryOption($option, $this->worker->queries);
                    if (false !== $this->worker->redis->hSet('series', $seriesOption['key'], serialize($seriesOption)) && false !== $this->worker->redis->hSet('queries', $key, serialize($option)))
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
                        $this->sendMessageToAllWorker('task.reload', 1);
                    }
                    else
                    {
                        $data['status']  = 'error';
                        $data['message'] = '更新服务器失败, 请检查redis服务器';
                        goto send;
                    }

                    if (IS_DEBUG)
                    {
                        $this->debug("new option: ". print_r($option, true));
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
                if (isset($request->post['sql']))
                {
                    $sql    = $request->post['sql'];
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
                elseif (isset($request->post['key']))
                {
                    $key = $request->post['key'];
                }
                elseif (isset($request->get['key']))
                {
                    $key = $request->get['key'];
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
                    $this->sendMessageToAllWorker('task.reload', 1);

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
            case 'task/series':
                try
                {
                    $limit         = 100;
                    $page_type     = $request->get['page_type'];
                    $firstItem     = $request->get['first_item'];
                    $lastItem      = $request->get['last_item'];
                    $nextIterator  = (int)$request->get['next_iterator'];

                    if (!$this->worker->redis && !$this->worker->ssdb)
                    {
                        throw new Exception("请检查redis或ssdb服务是否开启!");
                    }

                    $datas = [];
                    if ($this->worker->isSSDB)
                    {
                        switch ($page_type)
                        {
                            case 'next':
                                $datas = $this->worker->ssdb->scan($lastItem, "z", $limit);
                                break;
                            case 'prev':
                                $datas = $this->worker->ssdb->rscan($firstItem, "z", $limit);
                                break;
                            default :
                                $datas = $this->worker->ssdb->scan('', "z", $limit);
                                break;
                        }
                    }else{
                        switch ($page_type)
                        {
                            case 'next':
                                $tempIterator = $nextIterator;
                                $key_arr      = $this->worker->redis->scan($tempIterator, "*", $limit);
                                $data['next_iterator'] = $tempIterator;
                                break;
                            default :
                                $tempIterator = '';
                                $key_arr      = $this->worker->redis->scan($tempIterator, "*", $limit);
                                $data['next_iterator'] = $tempIterator;
                                break;
                        }

                        if ($key_arr)
                        {
                            foreach($key_arr as $key)
                            {
                                $datas[$key] = $this->worker->redis->get($key);
                            }
                        }

                        $data['data_count'] = count($datas);
                    }

                    $list = array();
                    if ($datas) foreach($datas as $key => $item)
                    {
                        $temp_key  = explode(',', $key);
                        $temp_item = unserialize($item);

                        $result = '';
                        if (is_array($temp_item))
                        {
                            foreach ($temp_item as $k => $v)
                            {
                                $result .= $k.':'.json_encode($v)."<br/>";
                            }
                        }else{
                            $result .= $item;
                        }

                        $list[]     = array(
                            'tab'       => $temp_key[0],
                            'time_unit' => $temp_key[2],
                            'game'      => $temp_key[3],
                            'time'      => $temp_key[4],
                            'result'    => $result,
                        );
                    }

                    $data['status']    = 'ok';
                    $data['list']      = $list;
                    $data['is_ssdb']   = $this->worker->isSSDB;
                    $data['page_type'] = $page_type;
                    $data['limit']     = $limit;
                    $data['last_item'] = $key;
                }
                catch (Exception $e)
                {
                    $data['status']  = 'error';
                    $data['message'] = $e->getMessage();
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

                $this->debug('restart server by api from ip: '. $request->server['remote_addr']);

                # 200 毫秒后重启
                swoole_timer_after(200, function()
                {
                    $this->server->reload();
                });

                break;
            case 'series/edit':
                try
                {
                    $seriesKey = $request->post['key'];
                    $use       = $request->post['use']?true:false;
                    $allApp    = $request->post['allApp']?true:false;
                    $start     = $request->post['start'];
                    $end       = $request->post['end'];

                    if (!$seriesKey)
                    {
                        throw new Exception("缺失参数序列key!");
                    }

                    if ($this->worker->isSSDB)
                    {
                        $serieDetail = unserialize($this->worker->ssdb->hget('series', $seriesKey));
                    }
                    else
                    {
                        $serieDetail = unserialize($this->worker->redis->hget('series', $seriesKey));
                    }

                    if (!$serieDetail)
                    {
                        throw new Exception("无此序列信息详情!");
                    }

                    $serieDetail['use']    = $use;
                    $serieDetail['allApp'] = $allApp;
                    $serieDetail['start']  = $start;
                    $serieDetail['end']    = $end;

                    $doSet = $this->worker->redis->hSet('series', $seriesKey, serialize($serieDetail));

                    $data['status'] = 'ok';
                    $data['data']   = $doSet;
                }
                catch (Exception $e)
                {
                    $data['status']  = 'error';
                    $data['message'] = $e->getMessage();
                }
                break;
            case 'series/data':
                try
                {

                }
                catch (Exception $e)
                {
                    $data['status']  = 'error';
                    $data['message'] = $e->getMessage();
                }
                break;
            case 'data':
                # 获取指定统计的实时统计数据
                # 分组时间超过10分钟的数据每10分钟才会导出一次数据, 如果业务有需求当前实时的统计数据, 可以通过这个接口获取到
                # EXP: http://127.0.0.1:8000/api/data?key=fa76f679d916c270&app=test&type=1h&group=groupValue1,groupValue2
                try
                {
                    $queryKey = $request->get['key'];
                    if (!$queryKey)
                    {
                        throw new Exception('missing parameter key');
                    }

                    $app = $request->get['app'];
                    if (!$app)
                    {
                        $app = 'default';
                    }

                    $queryOption = $this->worker->redis->hGet('queries', $queryKey);
                    if (!$queryOption)
                    {
                        throw new Exception('can not found query: '. $queryKey);
                    }
                    $queryOption = unserialize($queryOption);
                    if (!$queryOption)
                    {
                        throw new Exception("query $queryKey data unserialize error");
                    }

                    $groupValue = $request->get['group'];
                    if ($groupValue)
                    {
                        $groupValue = str_replace(',', '_', $groupValue);
                        if (strlen($groupValue) > 60 || preg_match('#[^a-z0-9_\-]+#i', $groupValue))
                        {
                            # 分组拼接后 key 太长
                            # 有特殊字符
                            $groupValue = 'hash-' . md5($groupValue);
                        }
                    }
                    else
                    {
                        $groupValue = '';
                    }

                    if ($request->get['type'])
                    {
                        $timeOpt = $queryOption['groupTime'][$request->get['type']];
                        if (!$timeOpt)
                        {
                            throw new Exception('can not found group time: '. $request->get['type']);
                        }

                        $timeOptKey = $request->get['type'];
                    }
                    else
                    {
                        $timeOptKey = key($queryOption['groupTime']);
                        $timeOpt    = current($queryOption['groupTime']);
                    }

                    $time = time();
                    if ($timeOptKey === '-')
                    {
                        # 不分组
                        $timeKey = 0;
                    }
                    else
                    {
                        # 获取时间key, Exp: 20160610123
                        $timeKey = getTimeKey($time, $timeOpt[0], $timeOpt[1]);
                    }

                    # 算出唯一ID
                    $uniqueId = "{$queryOption['seriesKey']},$timeOptKey,$app,$timeKey,$groupValue";

                    # 获取任务ID
                    $taskId   = DataJob::getTaskId($uniqueId);

                    # 返回成功会是一个数组, 如果当前进程没有数据则返回 null, 如果读取失败则返回 false
                    $rs = $this->server->taskwait("total|$queryKey|$uniqueId", 2, $taskId);

                    if (false === $rs)
                    {
                        throw new Exception('get total data error');
                    }
                    elseif (-1 === $rs)
                    {
                        $rs = null;
                    }

                    $data['status'] = 'ok';
                    $data['data']   = $rs;
                }
                catch (Exception $e)
                {
                    $data['status']  = 'error';
                    $data['message'] = $e->getMessage();
                }

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
     * 创建一个序列设置, 如果存在则合并
     *
     * @param array $option
     * @param array $queries
     * @return array
     */
    public static function createSeriesByQueryOption($option, $queries)
    {
        $seriesKey = $option['seriesKey'];

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

        # 设置查询的映射
        foreach($queries as $k => $v)
        {
            if ($v['seriesKey'] == $seriesKey)
            {
                foreach ($v['groupTime'] as $t => $v2)
                {
                    $seriesOption['queries'][$t][] = $k;
                }

                # 合并函数
                foreach ($option['function'] as $fk => $fv)
                {
                    $seriesOption['function'][$fk] = array_merge($seriesOption['function'][$fk], $fv);
                }

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
        }

        foreach ($option['groupTime'] as $groupKey => $st)
        {
            if (!$seriesOption['queries'][$groupKey] || (is_array($seriesOption['queries'][$groupKey]) && !in_array($option['key'], $seriesOption['queries'][$groupKey])))
            {
                $seriesOption['queries'][$groupKey][] = $option['key'];
            }
        }

        return $seriesOption;
    }

    /**
     * @param Swoole\Server $server
     * @param $fromWorkerId
     * @param $message
     */
    public function onPipeMessage($server, $fromWorkerId, $message, $serverId = -1)
    {
        switch ($message)
        {
            case 'task.reload':
                # 更新配置
                $this->worker->reloadSetting();
                break;

            case 'pause':
                # 暂停接受任何数据
                $this->worker->pause();
                break;

            case 'continue':
                # 继续接受数据
                $this->worker->stopPause();

                break;
        }
    }
}