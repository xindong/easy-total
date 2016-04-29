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
        include $file;
        include __DIR__ .'/../admin/_footer.php';
        $html = ob_get_clean();

        $this->response->end($html);
    }

    protected function api($uri)
    {
        $data = [];

        switch ($uri)
        {
            case 'task/add':
            case 'task/merge':
                # 添加一个任务
                $sql = $this->request->post['sql'];

                if (!$this->worker->redis)
                {
                    $data['status']  = 'error';
                    $data['message'] = 'redis server is not active';
                    goto send;
                }

                if (!$sql)
                {
                    $data['status']  = 'error';
                    $data['message'] = 'need parameter sql';
                    goto send;
                }

                if ($option = self::parseSql($sql))
                {
                    $key    = $option['key'];
                    $table  = $option['table'];
                    $saveAs = key($option['saveAs']);
                    $name   = trim($this->request->post['name']) ?: '';

                    if ($name)
                    {
                        $option['setting'][$saveAs]['name'] = $name;
                    }
                    $option['setting'][$saveAs]['time'] = time();

                    if (isset($this->worker->tasks[$table][$key]))
                    {
                        $oldOpt = $this->worker->tasks[$table][$key];

                        if (isset($oldOpt['saveAs'][$saveAs]))
                        {
                            # 已经存在一个
                            if ($uri === 'task/add')
                            {
                                $data['status']  = 'error';
                                $data['message'] = "the task from {$table} and save as {$saveAs} already exists. you can use api task/merge update the exists task";

                                goto send;
                            }
                            else
                            {
                                $oldSql = $oldOpt['sqlOrigin'][$saveAs];

                                # 合并所有配置
                                $option = self::mergeOption($oldOpt, $option);

                                # 处理合并后的SQL
                                $option['sql'][$saveAs]       = self::getSqlByOption($oldOpt);
                                $option['sqlOrigin'][$saveAs] = "$oldSql;\n{$oldOpt['sqlOrigin'][$saveAs]}";
                            }
                        }
                        else
                        {
                            $option = self::mergeOption($oldOpt, $option);
                        }
                    }

                    if (false !== $this->worker->redis->hSet('queries', $key, serialize($option)))
                    {
                        # 通知所有worker进程更新SQL
                        for ($i = 0; $i < $this->server->setting['worker_num']; $i++)
                        {
                            # 每个服务器通知更新
                            if ($i !== $this->workerId)
                            {
                                $msg = [
                                    'type' => 'task.update',
                                    'key'  => $key,
                                ];
                                $this->server->sendMessage(json_encode($msg), $i);
                            }
                        }

                        $this->worker->tasks[$table][$key] = $option;
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
                    $data['saveAs'] = $saveAs;
                    $data['sql']    = $option['sql'][$data['saveAs']];

                    info("fork new sql($key): {$data['sql']}");
                }
                else
                {
                    $data['status']  = 'error';
                    $data['message'] = 'parse sql error';
                }

                break;

            case 'task/remove':
                # 添加一个任务
                $option = null;
                if (isset($this->request->post['sql']))
                {
                    $sql    = $this->request->post['sql'];
                    $option = self::parseSql($sql);
                    if (!$option)
                    {
                        $data['status']  = 'error';
                        $data['message'] = 'parse sql error';

                        goto send;
                    }

                    $key   = $option['key'];
                    $table = $option['table'];
                    $save  = key($option['saveAs']);
                }
                elseif (isset($this->request->post['key']) && isset($this->request->post['table']))
                {
                    $key   = $this->request->post['key'];
                    $table = $this->request->post['table'];
                    $save  = $this->request->post['saveAs'] ?: $table;
                }
                elseif (isset($this->request->get['key']) && isset($this->request->post['table']))
                {
                    $key   = $this->request->get['key'];
                    $table = $this->request->get['table'];
                    $save  = $this->request->get['saveAs'] ?: $table;
                }
                else
                {
                    $data['status']  = 'error';
                    $data['message'] = 'need arguments [key and table] or [sql]';
                    goto send;
                }

                if (isset($key) && isset($table))
                {
                    if (isset($this->worker->tasks[$table][$key]))
                    {
                        $option = $this->worker->tasks[$table][$key];
                    }
                    else
                    {
                        $data['status']  = 'error';
                        $data['message'] = "can not found (key={$key},table={$table},saveAs={$save}) task";
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
                        unset($option['sqlOrigin'][$save]);

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

                    # 清理redis中的数据
                    $this->worker->clearDataByKey($key);

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
     * 解析一个SQL语句
     *
     * @param $sql
     * @return array|bool
     */
    protected static function parseSql($sql)
    {
        $preg = "#^select[ ]+(?<select>.+) from (?:(?<app>[a-z0-9_]+)\.)?(?<table>[a-z0-9_]+)(?:[ ]+for[ ]+(?<for>[a-z0-9,]+))?(?: where (?<where>(?:(?! group[ ]+time | group[ ]+by | save[ ]+as ).)+))?(?: group[ ]+by[ ]+(?<groupBy>[a-z0-9_,]+))?(?: group[ ]+time[ ]+(?<groupTime>\d+(?:d|h|m|s|W)))?(?: save[ ]+as (?<saveAs>[a-z0-9_]+))?$#i";
        if (preg_match($preg, $sql, $m))
        {
            if (IS_DEBUG)
            {
                echo "Match: ";
                print_r($m);
            }

            $table     = trim($m['table']);
            $select    = trim($m['select']);
            $for       = trim($m['for']);
            $where     = trim($m['where']);
            $groupBy   = trim($m['groupBy']);
            $groupTime = trim($m['groupTime']);
            $saveAs    = trim($m['saveAs']) ?: $table;
            $option    = [
                'key'   => null,
                'table' => $table,
                'use'   => true,
                'sql'   => [],
                'sqlOrigin' => [
                    $saveAs => $sql
                ]
            ];

            if ($select === '*')
            {
                $option['allField'] = $option['saveAs'][$saveAs]['allField'] = true;
            }
            else
            {
                foreach (explode(',', $select) as $s)
                {
                    $s = trim($s);
                    if ($s === '*')
                    {
                        $option['allField'] = $option['saveAs'][$saveAs]['allField'] = true;
                    }
                    elseif (preg_match('#^(?<field>[a-z0-9_]+)(?:[ ]+as[ ]+(?<as>[a-z0-9_]+))?(?:[ ]+)?$#i', $s, $mSelect))
                    {
                        # 匹配 select abc, abc as def
                        $field = trim($mSelect['field']);
                        $as    = trim($mSelect['as'] ?: $field);

                        $option['saveAs'][$saveAs]['field'][$as] = [
                            'type' => 'value',
                            'field' => $field,
                        ];

                        $option['function']['value'][$field] = true;
                    }
                    elseif (preg_match('#^(?<type>count|sum|max|min|avg|first|last|dist|exclude|listcount|list|value)[ ]*\((?<field>[a-z0-9_ \*]*)\)(?:[ ]+as[ ]+(?<as>[a-z0-9_]+))?$#i', $s, $mSelect))
                    {
                        # 匹配 select sum(abc), sum(abc) as def
                        $field = trim($mSelect['field']);
                        $type  = strtolower(trim($mSelect['type']));
                        $as    = trim($mSelect['as'] ?: $field);

                        if ($field === '*' && $type !== 'count')
                        {
                            # 只支持 count(*)
                            continue;
                        }

                        $option['saveAs'][$saveAs]['field'][$as] = [
                            'type'  => $type,
                            'field' => $field,
                        ];

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
                                $option['saveAs'][$saveAs]['field'][$as] = [
                                    'type'  => $type,
                                    'field' => '*',
                                ];
                                $option['function']['count']['*'] = true;
                                break;

                            default:
                                $option['function'][$type][$field] = true;
                                break;
                        }
                    }
                }
            }

            if ($for)
            {
                foreach (explode(',', $for) as $item)
                {
                    $option['for'][$item] = $item;
                }
                ksort($option['for']);
            }

            if ($where)
            {
                $option['where'] = self::parseWhere($where);
            }

            $GroupTimeSet = [
                'type'  => 'm',
                'limit' => 1,
            ];

            if ($groupTime)
            {
                if (preg_match('#^(\d+)(d|m|h|s)$#i', $groupTime, $m))
                {
                    $GroupTimeSet = [
                        'type'  => strtolower($m[2]),
                        'limit' => $m[1] >= 1 ? (int)$m[1] : ($m[2] == 's' ? 30 : 1),
                    ];
                }
                else
                {
                    debug("error group time: $groupTime, exp: 3m, 1d, 1h, 30s");
                }
            }

            $option['groupTime'] = $GroupTimeSet;

            if ($groupBy)
            {
                foreach(explode(',', $groupBy) as $item)
                {
                    $item = trim($item);
                    if ($item)
                    {
                        $option['groupBy'][] = trim($item);
                    }
                }

                if ($option['groupBy'])
                {
                    # 重新排序
                    sort($option['groupBy']);
                }
            }

            $option['key']          = self::getKeyByOption($option);
            $option['sql'][$saveAs] = self::getSqlByOption($option);

            return $option;
        }
        else
        {
            warn("error sql: $sql");
            return false;
        }
    }

    /**
     * 根据配置获取key
     *
     * @param $option
     * @return string
     */
    protected static function getKeyByOption($option)
    {
        $key = "table:{$option['table']}";

        if (isset($option['for']) && $option['for'])
        {
            $key .= '|for:' . implode(',', $option['for']);
        }

        if (isset($option['where']) && $option['where'])
        {
            $key .= '|where:' . $option['where']['$sql'];
        }

        if (isset($option['groupBy']) && $option['groupBy'])
        {
            $key .= ',' . implode(',', $option['groupBy']);
        }

        $key .= "|group:{$option['groupTime']['limit']}{$option['groupTime']['type']}";

        $key = substr(md5($key), 8, 16);

        return $key;
    }

    /**
     * 根据配置生成格式化后的SQL语句
     *
     * @param $option
     * @param null $table
     * @return bool|string
     */
    protected static function getSqlByOption($option, $table = null)
    {
        if (null === $table)
        {
            $table = key($option['saveAs']);
        }
        elseif (!isset($option['saveAs'][$table]))
        {
            return false;
        }

        $save = $option['saveAs'][$table];


        $select = [];
        if ($save['allField'])
        {
            $select[] = '*';
        }

        foreach ($save['field'] as $as => $st)
        {
            if ($st['type'] === 'value')
            {
                $tmp = $st['field'];
            }
            else
            {
                $tmp = "{$st['type']}({$st['field']})";
            }

            if ($st['field'] !== $as)
            {
                $tmp .= " as {$as}";
            }

            $select[] = $tmp;
        }

        $sql = 'select '. implode(',', $select) . " from {$option['table']}";

        if (isset($option['for']) && $option['for'])
        {
            $sql .= " for ". implode(',', $option['for']);
        }

        if (isset($option['where']) && $option['where'])
        {
            $sql .= " where {$option['where']['$sql']}";
        }

        if (isset($option['groupBy']) && $option['groupBy'])
        {
            $sql .= " group by ". implode(',', $option['groupBy']);
        }

        $sql .= " group time {$option['groupTime']['limit']}{$option['groupTime']['type']}";

        if ($table !== $option['table'])
        {
            $sql .= " save as {$table}";
        }

        return $sql;
    }

    protected static function mergeOption($opt1, $opt2)
    {
        foreach ($opt2 as $key => $item)
        {
            if (is_array($item))
            {
                $opt1[$key] = self::mergeOption($opt1[$key], $item);
            }
            else
            {
                $opt1[$key] = $item;
            }
        }

        return $opt1;
    }

    /**
     * 解析一个where字符串为一个多维结构数组
     *
     * 例如:
     *
     *      ((a < 1 and b % 3 = 2 and (aa=1 or bb=2 or (cc=3 and dd=4))) or ccc = 3) and (aaaa=1 or bbbb=2)
     *
     * @param $where
     * @return array
     */
    protected static function parseWhere($where)
    {
        $funHash = [];

        $parseWhere = function($where) use (& $funHash)
        {
            if (preg_match('#^(?<field>[a-z0-9_\'`"]+)(?:(?:[ ]+)?(?<typeM>%|>>|<<|mod|func|\-|\+|x|\*|/)(?:[ ]+)?(?<mValue>[0-9a-z]+))?(?:[ ]+)?(?<type>=|\!=|\<\>|\>|\<)(?:[ ]+)?(?<value>.*)$#i', $where , $mWhere))
            {
                $field  = self::deQuoteValue($mWhere['field']);
                $type   = $mWhere['type'] === '<>' ? '!=' : $mWhere['type'];
                $value  = self::deQuoteValue($mWhere['value']);
                $typeM  = $mWhere['typeM'];
                $mValue = $mWhere['mValue'];

                if ($typeM === 'func')
                {
                    # time_format(a, '%Y%m') = 201601
                    if (isset($funHash[$field]))
                    {
                        $opt = $funHash[$field];
                        $field = $opt['field'];
                    }
                    else
                    {
                        return false;
                    }
                }

                $option = [
                    '$sql'  => $field .($typeM ? " $typeM ". $mValue:'') . " $type " . $value,
                    'field' => $field,
                    'type'  => $type,
                    'value' => $value,
                    'typeM' => $typeM,
                    'mValue'=> $mValue,
                ];

                if ($typeM === 'func' && isset($opt))
                {
                    $option['arg']  = $opt['arg'];
                    $option['fun']  = $opt['fun'];

                    if ($opt['fun'] === 'in')
                    {
                        $option['$sql']  = "$field in(". implode(',', $opt['arg']) .")";
                    }
                    elseif ($opt['fun'] === 'not_in')
                    {
                        $option['$sql'] = "$field not in(". implode(',', $opt['arg']) .")";
                    }
                    else
                    {
                        $option['$sql'] = "{$opt['fun']}($field" . ($opt['arg'] ? ', \'' . $opt['arg'] . "'" : '') . ") {$type} {$value}";
                    }
                }

                return $option;
            }

            return false;
        };

        $where = preg_replace('# and #i', ' && ', preg_replace('# or #i', ' || ', $where));

        # 预处理函数
        if (preg_match_all('#(?<fun>[a-z_0-9]+)\((?<field>[a-z0-9_"\'` ])(?:(?>[ ]+)?,(?>[ ]+)?(?<arg>[^\)]+))?\)#Ui', $where, $m))
        {
            foreach ($m[0] as $k => $v)
            {
                $hash = md5($v);

                $funHash[$hash] = [
                    'fun'   => strtolower($m['fun'][$k]),
                    'field' => self::deQuoteValue($m['field'][$k]),
                    'arg'   => self::deQuoteValue($m['arg'][$k]),
                ];

                $where = str_replace($v, "{$hash} func 0 = 0", $where);
            }
        }

        # 解析in, not in
        if (preg_match_all('#(?<field>[a-z0-9]+)[ ]+(?<notIn>not[ ]+)?in[ ]*\((?<arg>.+)\)#Ui', $where, $m))
        {
            foreach ($m[0] as $k => $v)
            {
                $hash = md5($v);

                $arg  = explode(',', $m['arg'][$k]);
                $arg  = array_map('self::deQuoteValue', $arg);
                $arg  = array_unique($arg);
                sort($arg);

                $funHash[$hash] = [
                    'fun'   => $m['notIn'][$k] ? 'not_in' : 'in',
                    'field' => self::deQuoteValue($m['field'][$k]),
                    'arg'   => $arg,
                ];

                $where = str_replace($v, "{$hash} func 0 = 0", $where);
            }
        }

        $len        = strlen($where);
        $groupLevel = 0;
        $tmpWhere   = '';
        $whereArr   = [];
        $whereGroup = '&&';
        $nextGroup  = null;

        for ($i = 0; $i < $len; $i++)
        {
            $subStr = $where[$i];
            $tmpGroupLevel = $groupLevel;

            if ($nextGroup)
            {
                $whereGroup = $nextGroup;
                $nextGroup  = null;
            }

            if (in_array($subPot = substr($where, $i, 4), [' && ', ' || ']))
            {
                $nextGroup = trim($subPot);
            }
            else
            {
                $nextGroup = null;
            }

            if ($nextGroup)
            {
                $whereStr = $tmpWhere;
                $tmpWhere = '';
            }
            elseif ($subStr === '(')
            {
                $groupLevel++;
                $whereStr = $tmpWhere;
                $tmpWhere = '';
            }
            elseif ($subStr === ')')
            {
                $groupLevel--;
                $whereStr = $tmpWhere;
                $tmpWhere = '';
            }
            elseif ($i + 1 === $len)
            {
                $tmpWhere .= $subStr;
                $whereStr = $tmpWhere;
            }
            else
            {
                $whereStr = '';
                $tmpWhere .= $subStr;
            }

            if ($whereStr)
            {
                $whereStr = trim($whereStr);
                if (preg_match('#^(&&|\|\|) (.*)$#', $whereStr, $m))
                {
                    $whereArr[] = [
                        'level'    => $tmpGroupLevel,
                        'type'     => $nextGroup ?: $whereGroup,
                        'query'    => $m[1],
                    ];
                    $whereStr = trim($m[2]);
                }
                $whereArr[] = [
                    'level'    => $tmpGroupLevel,
                    'type'     => $nextGroup ?: $whereGroup,
                    'query'    => $whereStr,
                ];
            }
        }

        $tmpLevel = 0;
        $tmpArr   = [
            '$type'  => '&&',
            '$level' => 0,
            '$sql'   => '',
            '$item'  => [],
        ];
        $tmpArrList  = [];
        $whereOption =& $tmpArr;
        $tmpType     = '&&';

        foreach ($whereArr as $item)
        {
            if ($item['level'] < $tmpLevel)
            {
                # 上一级
                for ($j = 0; $j < $tmpLevel - $item['level']; $j++)
                {
                    end($tmpArrList);
                    $key = key($tmpArrList);
                    unset($parentArr);
                    $parentArr =& $tmpArrList[$key];
                    unset($tmpArrList[$key]);
                }

                $tmpArr =& $parentArr;
            }
            elseif ($item['level'] > $tmpLevel)
            {
                # 下一级
                for ($j = 0; $j < $item['level'] - $tmpLevel; $j++)
                {
                    unset($tmpArrOld);
                    $tmpArrOld =& $tmpArr;
                    unset($tmpArr);
                    $tmpArr = [
                        '$type'  => $item['type'],
                        '$level' => $item['level'],
                        '$sql'   => '',
                        '$item'  => [],
                    ];
                    $tmpArrOld['$item'][]  =& $tmpArr;
                    if (isset($tmpArrOld['$item'][0]))
                    {
                        $tmpArrList[] =& $tmpArrOld;
                    }
                }
            }
            elseif ($tmpType !== $item['type'])
            {
                # 类型不相同
                unset($tmpArrOld);
                $tmpArrOld =& $tmpArr;
                unset($tmpArr);
                $tmpArr = [
                    '$type'  => $item['type'],
                    '$level' => $item['level'],
                    '$sql'   => '',
                    '$item'  => [],
                ];
                if (isset($tmpArrOld['$item'][0]))
                {
                    $tmpArr['$item'][] =& $tmpArrOld;
                }
            }

            if ($item['query'] === '&&' || $item['query'] === '||')
            {
                if ($tmpArr['$type'] !== $item['query'])
                {
                    # 和前面分组不一样
                    if (isset($tmpArr['$item'][1]))
                    {
                        # 已经有了2个, 需要新建一个分组
                        unset($tmpArrOld);
                        $tmpArrOld =& $tmpArr;
                        unset($tmpArr);
                        $tmpArr = [
                            '$type'  => $item['query'],
                            '$level' => $item['level'],
                            '$sql'   => '',
                            '$item'  => [],
                        ];
                        if (isset($tmpArrOld['$item'][0]))
                        {
                            $tmpArr['$item'][] =& $tmpArrOld;
                        }
                    }
                    else
                    {
                        $tmpArr['$type'] = $item['query'];
                    }
                }
            }
            else
            {
                if ($tmpOpt = $parseWhere($item['query']))
                {
                    $tmpArr['$item'][] = $tmpOpt;
                }
            }

            $tmpLevel = $item['level'];
            $tmpType  = $item['type'];
        }

        if (count($tmpArrList) === 0)
        {
            unset($whereOption);
            $whereOption = $tmpArr;
        }

        $whereOption = self::whereOptionFormat($whereOption);

        return $whereOption;
    }

    protected static function whereOptionFormat($option)
    {
        # 处理排序
        $sort = function($a, $b)
        {
            $arr = [$a['$sql'], $b['$sql']];
            sort($arr);

            return $a['$sql'] === $arr[0] ? -1 : 1;
        };

        if ($option['$type'] !== 'where' && $option['$item'])
        {
            foreach ($option['$item'] as $k => & $item)
            {
                if (isset($item['$type']))
                {
                    $item = self::whereOptionFormat($item);

                    if (count($item['$item']) === 0)
                    {
                        # 移除空的数据
                        unset($option['$item'][$k]);
                    }
                }
            }
            unset($item);

            usort($option['$item'], $sort);

            $sql = [];
            foreach ($option['$item'] as $tmp)
            {
                $sql[] = isset($tmp['$type']) ? '('.$tmp['$sql'].')' : $tmp['$sql'];
            }

            $option['$sql'] = implode(" {$option['$type']} ", $sql);
        }

        return $option;
    }

    public static function deQuoteValue($value)
    {
        return preg_replace('#^`(.*)`$#', '$1', preg_replace('#^"(.*)"$#', '$1', preg_replace("#^'(.*)'$#", '$1', trim($value))));
    }
}