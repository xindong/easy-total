<?php
class SQL
{
    /**
     * 解析一个SQL语句
     *
     * @param $sql
     * @return array|bool
     */
    public static function parseSql($sql)
    {
        # 单表join SELECT * FROM t1 LEFT JOIN t2 ON t2.a=t1.a
        # 多表join SELECT * FROM t1 LEFT JOIN (t2, t3, t4) ON (t2.a=t1.a AND t3.b=t1.b AND t4.c=t1.c)

        $tpl   = "[a-z0-9_'`\"]+";
        $preg  = "#^select[ ]+(?<select>.+)[ ]+";
        $preg .= "from (?:(?<app>$tpl)\.)?(?<table>$tpl)(?:[ ]+as[ ]+(?<tableAs>$tpl))?";
        $preg .= "(?:[ ]+for[ ]+(?<for>[a-z0-9_,`'\"]+))?";
        $preg .= "(?:[ ]+";
        $preg .= "(?<leftJoin>left[ ]+)?join[ ]+(?<join>[a-z0-9,_` \)\(]+)";
        $preg .= "[ ]+on (?<on>(?:(?! where| group[ ]+time | group[ ]+by | save[ ]+as ).)+)";
        $preg .= ")?";
        $preg .= "(?:[ ]+where (?<where>(?:(?! group[ ]+time | group[ ]+by | save[ ]+as ).)+))?";
        $preg .= "(?:[ ]+group[ ]+by[ ]+(?<groupBy>[a-z0-9_\.,`]+))?";
        $preg .= "(?:[ ]+group[ ]+time[ ]+(?<groupTime>(?:(?! save[ ]+as).)+))?";
        $preg .= "(?:[ ]+save[ ]+as (?<saveAs>[a-z0-9_%,\.`]+))?$#i";

        if (preg_match($preg, preg_replace('#[ ]*,[ ]+#', ',', str_replace(["\r\n", "\r", "\n"], ' ', $sql)), $m))
        {
            if (IS_DEBUG)
            {
                echo "Match: ";
                print_r($m);
            }

            if (isset($m['join']) && $m['join'])
            {
                # join 模式
                $joinOption = self::parseJoin($m);

                if (false === $joinOption)
                {
                    warn("sql join option error: join = {$m['join']}, on = {$m['join']}");
                    return false;
                }
            }
            else
            {
                $joinOption = null;
            }

            $app        = self::deQuoteValue($m['app']);
            $table      = self::deQuoteValue($m['table']);
            $tableAs    = self::deQuoteValue($m['tableAs']) ?: $table;
            $select     = trim($m['select']);
            $for        = trim($m['for']);
            $where      = trim($m['where']);
            $groupBy    = trim($m['groupBy']);
            $groupTime  = trim($m['groupTime']);
            $saveAs     = self::deQuoteValue(str_replace('`', '', $m['saveAs'])) ?: $table;

            $option    = [
                'key'        => substr(md5($sql .'_'. microtime(1)), 8, 16),    //任务的key
                'name'       => "from {$table} to {$saveAs}",
                'use'        => true,    //是否开启
                'createTime' => time(),
                'editTime'   => 0,
                'deleteTime' => 0,
                'seriesKey'  => null,    //序列的key, 生成好完整的option后再赋值
                'sql'        => '',      //SQL语句
                'table'      => $table,
                'tableAs'    => $tableAs,
                'for'        => [],
                'saveAs'     => [],      //保存的设置
                'start'      => 0,       //开启时间
                'end'        => 0,       //结束时间
                'allField'   => false,
                'fields'     => [],      //导出的字段设置
                'where'      => $where ? self::parseWhere($where, $joinOption) : [],    //where条件
                'groupTime'  => [],      //时间分组设置
                'groupBy'    => [],      //字段分组设置
                'function'   => [],      //所有使用到的方法列表
            ];

            if ($joinOption)
            {
                $option['join'] =& $joinOption;
            }

            # 解析SELECT部分
            if (!self::parseSelect($select, $option))
            {
                return false;
            }

            if ($app)
            {
                $option['for'][$app] = $app;
            }
            if ($for)
            {
                foreach (explode(',', $for) as $item)
                {
                    $item = self::deQuoteValue($item);
                    $option['for'][$item] = $item;
                }
                ksort($option['for']);
            }

            if ($groupBy)
            {
                foreach(explode(',', $groupBy) as $item)
                {
                    if (strpos($item, '.'))
                    {
                        list($space, $tmpItem) = explode('.', $item, 2);

                        $space = self::deQuoteValue($space);
                        $item  = self::deQuoteValue($tmpItem);

                        if (isset($joinOption['join'][$space]))
                        {
                            $joinOption['fields'][$joinOption['join'][$space]][] = $item;
                            $item = $joinOption['join'][$space] . '.' . $item;
                        }
                    }
                    else
                    {
                        $item = self::deQuoteValue($item);
                    }

                    if ($item)
                    {
                        $option['groupBy'][] = $item;
                    }
                }

                if ($option['groupBy'])
                {
                    # 重新排序
                    sort($option['groupBy']);
                }
            }

            if ($joinOption)
            {
                # 对join中用到的字段去重并重新排序
                foreach ($joinOption['fields'] as $k => &$v)
                {
                    $v = array_unique($v);
                    sort($v);
                }
                unset($v);
            }

            $groupTimeSet = [];
            if ($groupTime)
            {
                foreach (explode(',', trim($groupTime)) as $item)
                {
                    $item = self::deQuoteValue($item);
                    if ($item === 'none')
                    {
                        $groupTimeSet['-'] = [0, '-'];
                    }
                    elseif (preg_match('#^(\d+)([a-z]+)$#i', $item, $m))
                    {
                        if ($m[2] !== 'M')$m[2] = strtolower($m[2]);

                        switch ($m[2])
                        {
                            case 'year':
                            case 'y':
                                $m[2] = 'y';
                                break;

                            case 'month':
                            case 'm':
                                $m[2] = 'm';
                                break;

                            case 'week':
                            case 'w':
                                $m[2] = 'w';
                                break;

                            case 'day':
                            case 'd':
                                $m[2] = 'd';
                                break;

                            case 'hour':
                            case 'h':
                                $m[2] = 'h';
                                break;

                            case 'minutes':
                            case 'M':
                            case 'i':
                                $m[2] = 'i';
                                break;

                            case 'seconds':
                            case 's':
                                $m[2] = 's';
                                break;

                            default:
                                debug("error group time: $item, exp: 3M, 1d, 1h, 30s");
                                continue 2;
                        }

                        $set = [
                            $m[1] >= 1 ? (int)$m[1] : ($m[2] == 's' ? 30 : 1),
                            $m[2],
                        ];

                        $groupTimeSet[$set[0].$set[1]] = $set;
                    }
                    else
                    {
                        debug("error group time: $groupTime, exp: 3M, 1d, 1h, 30s");
                    }
                }
            }
            # 设定时间分组
            $option['groupTime'] = $groupTimeSet ?: ['1i' => [1, 'i', 60]];

            # 根据时间分组设置输出表设置
            $saveAsArr = explode(',', str_replace(' ', '', $saveAs));
            $current   = $saveAsArr[0];
            $i         = 0;
            foreach ($option['groupTime'] as $k => $v)
            {
                if (isset($saveAsArr[$i]) && $saveAsArr[$i])
                {
                    $current = $saveAsArr[$i];
                    if (strpos($current, '%') !== false && preg_match_all('#%([a-z])#i', $current, $m))
                    {
                        # 得到所有 % 开头的变量
                        $current = [
                            $current,                             // 当前设置
                            'date',                               // 处理类型, 方便以后扩展功能
                            array_values(array_unique($m[0])),    // 所有需要替换的字符串
                            implode(',', array_unique($m[1])),    // 对应的要替换的时间字符串
                        ];
                    }
                }

                $option['saveAs'][$k] = $current;
                $i++;
            }

            $option['sql']       = self::getSqlByOption($option);
            $option['seriesKey'] = self::getSeriesKeyByOption($option);

            return $option;
        }
        else
        {
            warn("error sql: $sql");
            return false;
        }
    }

    /**
     * 根据配置序列的key
     *
     * @param $option
     * @return string
     */
    public static function getSeriesKeyByOption($option)
    {
        $key = "table:{$option['table']}";

        if (isset($option['where']) && $option['where'])
        {
            $key .= '|where:' . $option['where']['$sql'];
        }

        if (isset($option['groupBy']) && $option['groupBy'])
        {
            $key .= '|group:' . implode(',', $option['groupBy']);
        }

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
    public static function getSqlByOption($option)
    {
        $select = [];
        if ($option['allField'])
        {
            $select[] = '*';
        }

        if (isset($option['join']) && $option['join'])
        {
            $joinMap = array_flip($option['join']['join']);
        }
        else
        {
            $joinMap = [];
        }

        $saveOption = $option['fields'];
        foreach ($saveOption as $as => $opt)
        {
            $field = $opt['field'];

            if ($opt['type'] === 'dist' && strpos($field, ','))
            {
                # 多字段
                $tmp2 = explode(',', $field);
                foreach ($tmp2 as & $tt)
                {
                    if (strpos($tt, '.'))
                    {
                        $tmp = explode('.', $tt, 2);
                        if (isset($joinMap[$tmp[0]]))
                        {
                            $tt = "`{$joinMap[$tmp[0]]}`.`{$tmp[1]}`";
                        }
                        else
                        {
                            $tt = "`{$tmp[1]}`";
                        }
                    }
                    else
                    {
                        $tt = "`$tt`";
                    }
                }
                unset($tt);

                $field = implode(', ', $tmp2);
            }
            elseif (strpos($field, '.'))
            {
                $tmp = explode('.', $field, 2);
                if (isset($joinMap[$tmp[0]]))
                {
                    $field = "`{$joinMap[$tmp[0]]}`.`{$tmp[1]}`";
                }
                else
                {
                    $field = "`{$tmp[1]}`";
                }
            }
            elseif ($field !== '*')
            {
                $field = "`$field`";
            }

            if ($opt['type'] === 'value')
            {
                $tmp = $field;
            }
            else
            {
                $tmp = strtoupper($opt['type']) . "({$field})";
            }

            if ($opt['field'] !== $as)
            {
                $tmp .= " AS `{$as}`";
            }

            $select[] = $tmp;
        }

        $sql = 'SELECT '. implode(', ', $select) . " FROM `{$option['table']}`";
        if ($option['tableAs'] != $option['table'])
        {
            $sql .= " AS `{$option['tableAs']}`";
        }

        if (isset($option['for']) && $option['for'])
        {
            $sql .= " FOR ". implode(',', array_map(function($v){return "`$v`";}, $option['for']));
        }

        if (isset($option['join']) && $option['join'])
        {
            $sql .= ' ' . $option['join']['$sql'];
        }

        if (isset($option['where']) && $option['where'])
        {
            $sql .= " WHERE {$option['where']['$sql']}";
        }

        if (isset($option['groupBy']) && $option['groupBy'])
        {
            $groupBy = $option['groupBy'];

            foreach ($groupBy as & $item)
            {
                if (strpos($item, '.'))
                {
                    $tmp  = explode('.', $item, 2);
                    if ($joinMap && isset($joinMap[$tmp[0]]))
                    {
                        $item = "`{$joinMap[$tmp[0]]}`.`{$tmp[1]}`";
                    }
                    else
                    {
                        $item = "`{$tmp[1]}`";
                    }
                }
                else
                {
                    $item = "`{$item}`";
                }
            }

            $sql .= " GROUP BY ". implode(', ', $groupBy);
        }

        $sql .= " GROUP TIME '". implode("', '", array_keys($option['groupTime'])) ."'";

        if ($option['save'][0] !== $option['table'] || count($option['save']) > 1)
        {
            $saveAs = $option['saveAs'];
            foreach ($saveAs as $k => &$v)
            {
                if (is_array($v))
                {
                    $v = $v[0];
                }
                $v = "`$v`";
            }
            $sql .= " SAVE AS ". implode(', ', $saveAs);
        }

        return $sql;
    }

    public static function mergeOption($opt1, $opt2)
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

    protected static function parseSelect($select, & $option)
    {
        if ($select === '*')
        {
            $option['allField'] = true;
        }
        else
        {
            $tpl        = "[a-z0-9_'`\"]+";
            $nextStep   = '';
            $joinOption =& $option['join'] ?: [];

            foreach (explode(',', $select) as $s)
            {
                if ($nextStep)
                {
                    if (preg_match('#[a-z0-9\._\'`"]+\)(?:[ ]+as[ ]+'. $tpl .')?#i', trim($s)))
                    {
                        $s = $nextStep .','. $s;
                        $nextStep = '';
                    }
                    else
                    {
                        $nextStep .= ','. $s;
                        continue;
                    }
                }
                elseif (preg_match('#^[a-z0-9\._\'"` ]+\([^\)]+$#i', $s, $m))
                {
                    # 如果没有遇到封闭函数, 则可能是 select dist(a,b),c 这样被, 分开了
                    $nextStep = $s;
                    continue;
                }

                $s = trim($s);

                if ($s === '*')
                {
                    $option['allField'] = true;
                }
                elseif (preg_match("#^(?:(?<space>$tpl)\\.)?(?<field>$tpl)(?:[ ]+as[ ]+(?<as>$tpl))?(?:[ ]+)?$#i", $s, $mSelect))
                {
                    # 匹配 select abc, abc as def
                    $field = self::deQuoteValue($mSelect['field']);
                    $as    = self::deQuoteValue($mSelect['as'] ?: $field);

                    # 处理 select a.abc 的情形
                    if (isset($mSelect['space']))
                    {
                        $space = self::deQuoteValue($mSelect['space']);
                        if ($space)
                        {
                            if (isset($joinOption['join'][$space]))
                            {
                                $joinOption['fields'][$joinOption['join'][$space]][] = $field;
                                $field = "{$joinOption['join'][$space]}.$field";
                            }
                            elseif ($space !== $option['tableAs'])
                            {
                                warn("select error #1: $s, can not found table space $space");

                                return false;
                            }
                        }
                    }
                    else
                    {
                        $space = false;
                    }

                    $option['fields'][$as] = [
                        'type'  => 'value',
                        'field' => $field,
                    ];

                    if ($space)
                    {
                        $option['fields'][$as]['space'] = 'join';
                    }

                    $option['function']['value'][$field] = true;
                }
                elseif (preg_match("#^(?<type>[a-z0-9_]+)[ ]*\\((?<field>[a-z0-9_\\., \\*\"'`]+)\\)(?:[ ]+as[ ]+(?<as>$tpl))?$#i", $s, $mSelect))
                {
                    # count|sum|max|min|avg|first|last|dist|exclude|listcount|list|value
                    # 匹配 select sum(abc), sum(abc) as def
                    $field = trim($mSelect['field'], " ,");
                    $type  = strtolower(trim($mSelect['type']));
                    $as    = str_replace(',', '_', self::deQuoteValue($mSelect['as'] ?: ($field === '*' ? $type : "`{$type}_{$field}`")));

                    if ($field === '*' && $type !== 'count')
                    {
                        # 只支持 count(*)
                        continue;
                    }

                    $isJoinField = false;
                    if ($type === 'dist' && false !== strpos($field, ','))
                    {
                        # Dist支持多字段模式
                        $fields = explode(',', $field);
                        foreach ($fields as & $item)
                        {
                            if (strpos($item, '.'))
                            {
                                list($space, $item) = explode('.', $item);

                                $space = self::deQuoteValue($space);
                                $item  = self::deQuoteValue($item);

                                if (isset($joinOption['join'][$space]))
                                {
                                    $joinOption['fields'][$joinOption['join'][$space]][] = $item;
                                    $item = "{$joinOption['join'][$space]}.$item";
                                }
                                elseif ($space !== $option['tableAs'])
                                {
                                    warn("select error #2: $field, can not found table space $space");
                                    return false;
                                }
                            }
                            else
                            {
                                $item = self::deQuoteValue($item);
                            }
                        }

                        unset($item);
                        sort($fields);
                        $field  = implode(',', $fields);
                    }
                    else
                    {
                        $fields = true;

                        if (preg_match("#^($tpl)\\.($tpl)$#i", $field, $m2))
                        {
                            $space = self::deQuoteValue($m2[1]);
                            $field = self::deQuoteValue($m2[2]);

                            if (isset($joinOption['join'][$space]))
                            {
                                $joinOption['fields'][$joinOption['join'][$space]][] = $field;
                                $field       = $joinOption['join'][$space] . '.' . $field;
                            }
                            elseif ($space !== $option['tableAs'])
                            {
                                warn("select error #3: $field, can not found table space $m2[1]");
                                return false;
                            }
                        }
                        else
                        {
                            $field = self::deQuoteValue($field);
                        }
                    }

                    $option['fields'][$as] = [
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
                            $option['function']['dist'][$field] = $fields;
                            break;

                        case 'list':
                        case 'listcount':
                            $option['function']['dist'][$field] = true;
                            break;

                        case 'count':
                            $option['fields'][$as] = [
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
                else
                {
                    warn("select error #4, unknown select: $s");
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 解析 JOIN ON 语句
     *
     * @param $m
     * @return array|bool
     */
    protected static function parseJoin($m)
    {
        if (!isset($m['on']))return false;
        $join = trim($m['join'], ' )(');
        $on   = trim($m['on'], ' )(');

        if (!$on)return false;

        $tmpJoin = [];
        foreach (explode(',', $join) as $item)
        {
            if (preg_match('#^([a-z0-9_`]+)[ ]+as[ ]+([a-z0-9_`]+)$#i', trim($item), $m1))
            {
                $f1 = self::deQuoteValue($m1[1]);
                $f2 = self::deQuoteValue($m1[2]);
            }
            else
            {
                $f1 = self::deQuoteValue($item);
                $f2 = $f1;
            }

            $tmpJoin[$f2] = $f1;
        }
        $join = $tmpJoin;
        unset($tmpJoin);

        $table   = self::deQuoteValue($m['table']);
        $tableAs = null;
        if (isset($m['tableAs']))
        {
            $tableAs = self::deQuoteValue($m['tableAs']);
        }
        if (!$tableAs)
        {
            $tableAs = $table;
        }

        $joinOption = [
            'table'    => $table,
            'tableAs'  => $tableAs,
            'leftJoin' => isset($m['leftJoin']) && $m['leftJoin'] ? true : false,
            'join'     => $join,
            'on'       => [],
            'fields'   => [],
            '$sql'     => '',
        ];
        $onWhere = self::parseWhere($on, $joinOption, true);

        # 拼接出SQL语句
        $sql = ($joinOption['leftJoin'] ? 'LEFT ' : '') . 'JOIN ';
        if (count($joinOption['join']) > 1)
        {
            $sql .= '(';
            foreach ($joinOption['join'] as $k => $v)
            {
                $sql .= "`$v`" . ($k === $v ? '' : " AS `$k`") .", ";
            }
            $sql = substr($sql, 0, -2) . ")";
        }
        else
        {
            $k    = key($joinOption['join']);
            $v    = current($joinOption['join']);
            $sql .= "`$v`" . ($k === $v ? '' : " AS `$k`");
        }

        foreach ($onWhere['$item'] as $item)
        {
            $k = [$item['field'], $item['value']];
            sort($k);
            $joinOption['on'][] = $k;
        }


        if (count($onWhere['$item']) > 1)
        {
            $sql .= " ON ({$onWhere['$sql']})";
        }
        else
        {
            $sql .= " ON {$onWhere['$sql']}";
        }

        $joinOption['$sql'] = $sql;

        return $joinOption;
    }


    /**
     * 解析一个where字符串为一个多维结构数组
     *
     * 例如:
     *
     *      ((a < 1 and b % 3 = 2 and (aa=1 or bb=2 or (cc=3 and dd=4))) or ccc = 3) and (aaaa=1 or bbbb=2)
     *
     * @param $where
     * @param $joinOption
     * @return array
     */
    protected static function parseWhere($where, & $joinOption, $joinWhere = false)
    {
        $funHash = [];

        $parseWhere = function($where) use (& $funHash, & $joinOption, $joinWhere)
        {
            if (preg_match('#^(?:(?<space>[a-z0-9_`]+)\.)?(?<field>[a-z0-9_`]+)(?:(?:[ ]+)?(?<typeM>%|>>|<<|mod|in|\-|\+|x|\*|/)(?:[ ]+)?(?<mValue>[0-9\.]+))?(?:[ ]+)?(?<type>=|\!=|\<\>|\>|\<)(?:[ ]+)?(?<value>.*)$#i', $where, $mWhere))
            {
                $space       = self::deQuoteValue($mWhere['space']);
                $field       = self::deQuoteValue($mWhere['field']);
                $type        = $mWhere['type'] === '<>' ? '!=' : $mWhere['type'];
                $typeM       = $mWhere['typeM'];
                $mValue      = $mWhere['mValue'];
                $valueString = trim($mWhere['value']);

                if ($joinWhere || preg_match('#^`(.*)`$#', $mWhere['value']))
                {
                    # 字段 = 字段模式
                    # where `field1` = `field2`
                    $value = $mWhere['value'];
                    $fMode = true;

                    if (strpos($value, '.'))
                    {
                        $tmp         = array_map('self::deQuoteValue', explode('.', $value, 2));
                        $valueString = "`{$tmp[0]}`.`{$tmp[1]}`";

                        if ($tmp[0] === $joinOption['tableAs'])
                        {
                            $value = "{$joinOption['table']}.{$tmp[1]}";
                        }
                        elseif (isset($joinOption['join'][$tmp[0]]))
                        {
                            $tmpTable = $joinOption['join'][$tmp[0]];
                            $value    = "$tmpTable.{$tmp[1]}";

                            $joinOption['fields'][$tmpTable][] = $tmp[1];
                        }
                        else
                        {
                            $value = implode('.', $tmp);
                        }
                    }
                    else
                    {
                        $value       = self::deQuoteValue($value);
                        $valueString = "`{$value}`";
                    }
                }
                else
                {
                    $value = self::deQuoteValue($mWhere['value']);
                    $fMode = false;
                }

                if ($space === '__fun__')
                {
                    # time_format(a, '%Y%m') = 201601
                    if (isset($funHash[$field]))
                    {
                        $opt   = $funHash[$field];
                        $field = $opt['field'];
                        $space = $opt['space'];
                    }
                    else
                    {
                        return false;
                    }
                    $typeM = 'func';
                }
                elseif ($typeM === 'in')
                {
                    if (isset($funHash[$field]))
                    {
                        $opt   = $funHash[$field];
                        $field = $opt['field'];
                        $space = $opt['space'];
                    }
                    else
                    {
                        return false;
                    }
                    $typeM = 'func';
                }

                if ($space)
                {
                    $fieldString = "`$space`.`$field`";

                    if ($space === $joinOption['tableAs'])
                    {
                        $field = "{$joinOption['table']}.{$field}";
                    }
                    elseif (isset($joinOption['join'][$space]))
                    {
                        $joinOption['fields'][$joinOption['join'][$space]][] = $field;
                        $field = "{$joinOption['join'][$space]}.{$field}";
                    }
                }
                else
                {
                    $fieldString = "`$field`";
                }

                $option = [
                    '$sql'  => $fieldString .($typeM ? " $typeM ". $mValue:'') . " $type " . $valueString,
                    'field' => $field,
                    'fMode' => $fMode,
                    'type'  => $type,
                    'value' => $value,
                    'typeM' => $typeM,
                    'mValue'=> $mValue,
                ];

                if ($typeM === 'func' && isset($opt))
                {
                    $option['arg'] = $opt['arg'];
                    $option['fun'] = $opt['fun'];
                    $arg           = $opt['argString'];

                    if ($opt['fieldArg'])
                    {
                        $option['field'] = $opt['fieldArg'];
                    }

                    if ($opt['fun'] === 'in')
                    {
                        $option['$sql']  = "$fieldString IN($arg)";
                    }
                    elseif ($opt['fun'] === 'not_in')
                    {
                        $option['$sql'] = "$fieldString NOT IN($arg)";
                    }
                    else
                    {
                        $option['$sql'] = "{$opt['fun']}({$arg}) {$type} {$valueString}";
                    }
                }

                return $option;
            }

            return false;
        };

        $parseFun = function($space, $field, $arg, $fun) use (& $joinOption)
        {
            $space     = self::deQuoteValue($space);
            $field     = self::deQuoteValue($field);
            $fieldArg  = [];
            $argString = [];

            if ($space && $field && isset($joinOption['join'][$space]))
            {
                # 这个是被join的表的字段
                $joinOption['fields'][$joinOption['join'][$space]][] = $field;
            }

            foreach ($arg as $i => & $tmp)
            {
                if (preg_match('#^`(.*)`$#', $tmp))
                {
                    # 字段模式
                    if (strpos($tmp, '.'))
                    {
                        list($space2, $field2) = explode('.', $tmp, 2);

                        $space2      = self::deQuoteValue($space2);
                        $field2      = self::deQuoteValue($field2);
                        $argString[] = "`$space2.$field2`";

                        if (isset($joinOption['join'][$space2]))
                        {
                            $joinOption['fields'][$joinOption['join'][$space2]][] = $field2;
                            $space2 = $joinOption['join'][$space2];
                        }

                        $tmp = "$space2.$field2";
                    }
                    else
                    {
                        $tmp          = self::deQuoteValue($tmp);
                        $argString[]  = "`$tmp`";
                    }

                    $fieldArg[$i] = $tmp;
                }
                else
                {
                    $tmp = self::deQuoteValue($tmp);
                    if (!is_numeric($tmp))
                    {
                        $argString[] = "'$tmp'";
                    }
                    else
                    {
                        $argString[] = $tmp;
                    }
                }
            }
            unset($tmp);
            $argString = implode(', ', $argString);

            return [
                'fun'       => $fun,
                'space'     => $space,
                'field'     => $field,
                'arg'       => $arg,
                'argString' => $argString,
                'fieldArg'  => $fieldArg,
            ];
        };

        $where = preg_replace('# and #i', ' && ', preg_replace('# or #i', ' || ', $where));

        # 解析in, not in
        if (preg_match_all('#(?:(?<space>[a-z0-9_`]+)\.)?(?<field>[a-z0-9`]+)[ ]+(?<notIn>not[ ]+)?in[ ]*\((?<arg>.+)\)#Ui', $where, $m))
        {
            foreach ($m[0] as $k => $v)
            {
                $hash = md5($v);
                $arg  = array_map('self::deQuoteValue', explode(',', $m['arg'][$k]));
                $arg  = array_unique($arg);
                sort($arg);
                $funHash[$hash] = $parseFun($m['space'][$k], $m['field'][$k], $arg, $m['notIn'][$k] ? 'not_in' : 'in');

                $where = str_replace($v, "{$hash} in 0 = 0", $where);
            }
        }

        # 预处理函数
        # Exp: from_unixtime(time, "%H") = 2016
        $match  = '#(?<fun>[a-z_0-9]+)\((?<arg>[^\)]+)\)#Ui';
        if (preg_match_all($match, $where, $m))
        {
            foreach ($m[0] as $k => $v)
            {
                $hash           = md5($v);
                $funHash[$hash] = $parseFun('', '', explode(',', $m['arg'][$k]), strtolower($m['fun'][$k]));

                if ('time_format' === $funHash[$hash]['fun'])
                {
                    $funHash[$hash]['fun'] = 'from_unixtime';
                }

                # 格式化成PHP的时间参数
                if ($funHash[$hash]['fun'] === 'from_unixtime')
                {
                    $caracs = [
                        '%d' => 'd',
                        '%a' => 'D',
                        '%e' => 'j',
                        '%A' => 'l',
                        '%u' => 'N',
                        '%w' => 'w',
                        '%j' => 'z',
                        '%V' => 'W',
                        '%B' => 'F',
                        '%m' => 'm',
                        '%b' => 'M',
                        '%G' => 'o',
                        '%Y' => 'Y',
                        '%y' => 'y',
                        '%P' => 'a',
                        '%p' => 'A',
                        '%l' => 'g',
                        '%I' => 'h',
                        '%H' => 'H',
                        '%M' => 'i',
                        '%S' => 's',
                        '%z' => 'O',
                        '%Z' => 'T',
                        '%s' => 'U',
                    ];
                    $funHash[$hash]['arg'][0] = strtr($funHash[$hash]['arg'][0], $caracs);
                }

                $where = str_replace($v, "__fun__.{$hash}", $where);
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