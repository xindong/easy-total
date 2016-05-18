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
        $preg  = "#^select[ ]+(?<select>.+) ";
        $preg .= "from (?:(?<app>[a-z0-9_]+)\.)?(?<table>[a-z0-9_]+)";
        $preg .= "(?:[ ]+for[ ]+(?<for>[a-z0-9,]+))?";
        $preg .= "(?: where (?<where>(?:(?! group[ ]+time | group[ ]+by | save[ ]+as ).)+))?";
        $preg .= "(?: group[ ]+by[ ]+(?<groupBy>[a-z0-9_,\.]+))?";
        $preg .= "(?: group[ ]+time[ ]+(?<groupTime>(?:(?! save[ ]+as).)+))?";
        $preg .= "(?: save[ ]+as (?<saveAs>[a-z0-9_%,\.]+))?$#i";

        if (preg_match($preg, str_replace(["\r\n", "\r", "\n"], ' ', $sql), $m))
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
                'key'        => substr(md5($sql .'_'. microtime(1)), 8, 16),    //任务的key
                'name'       => "from {$table} to {$saveAs}",
                'use'        => true,    //是否开启
                'createTime' => time(),
                'editTime'   => 0,
                'seriesKey'  => null,    //序列的key, 生成好完整的option后再赋值
                'sql'        => '',      //SQL语句
                'table'      => $table,
                'for'        => [],
                'saveAs'     => [],      //保存的设置
                'start'      => 0,       //开启时间
                'end'        => 0,       //结束时间
                'allField'   => false,
                'fields'     => [],      //导出的字段设置
                'where'      => $where ? self::parseWhere($where) : [],    //where条件
                'groupTime'  => [],      //时间分组设置
                'groupBy'    => [],      //字段分组设置
                'function'   => [],      //所有使用到的方法列表
            ];

            if ($select === '*')
            {
                $option['allField'] = true;
            }
            else
            {
                $nextStep = '';
                foreach (explode(',', $select) as $s)
                {
                    if ($nextStep)
                    {
                        if (preg_match('#[a-z0-9_]+\)(?:[ ]+as[ ]+[a-z0-9_]+)?#i', trim($s)))
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
                    elseif (preg_match('#^[a-z0-9_ ]+[ ]*\([^)]+$#', $s, $m))
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
                    elseif (preg_match('#^(?<field>[a-z0-9_]+)(?:[ ]+as[ ]+(?<as>[a-z0-9_]+))?(?:[ ]+)?$#i', $s, $mSelect))
                    {
                        # 匹配 select abc, abc as def
                        $field = trim($mSelect['field']);
                        $as    = trim($mSelect['as'] ?: $field);

                        $option['fields'][$as] = [
                            'type' => 'value',
                            'field' => $field,
                        ];

                        $option['function']['value'][$field] = true;
                    }
                    elseif (preg_match('#^(?<type>count|sum|max|min|avg|first|last|dist|exclude|listcount|list|value)[ ]*\((?<field>[a-z0-9_, \*"\'`]*)\)(?:[ ]+as[ ]+(?<as>[a-z0-9_]+))?$#i', $s, $mSelect))
                    {
                        # 匹配 select sum(abc), sum(abc) as def
                        $field = trim($mSelect['field'], " \n\r,");
                        $type  = strtolower(trim($mSelect['type']));
                        $as    = str_replace(',', '_', trim($mSelect['as'] ?: "{$type}_{$field}"));


                        if ($field === '*' && $type !== 'count')
                        {
                            # 只支持 count(*)
                            continue;
                        }

                        if ($type === 'dist' && false !== strpos($field, ','))
                        {
                            # Dist支持多字段模式
                            $fields = array_map('self::deQuoteValue', explode(',', $field));
                            sort($fields);
                            $field  = implode(',', $fields);
                        }
                        else
                        {
                            $fields = true;
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

            $groupTimeSet = [];
            if ($groupTime)
            {
                foreach (explode(',', trim($groupTime)) as $item)
                {
                    if ($item === 'none')
                    {
                        $groupTimeSet['none'] = true;
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
            $option['groupTime'] = $groupTimeSet ?: ['1i' => [1, 'i']];

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

        $saveOption = $option['fields'];
        foreach ($saveOption as $as => $opt)
        {
            if ($opt['type'] === 'value')
            {
                $tmp = $opt['field'];
            }
            else
            {
                $tmp = "{$opt['type']}({$opt['field']})";
            }

            if ($opt['field'] !== $as)
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

        $sql .= " group time ". implode(',', array_keys($option['groupTime']));

        if ($option['save'][0] !== $option['table'] || count($option['save']) > 1)
        {
            $sql .= " save as ". implode(',', $option['saveAs']);
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
                    $funHash[$hash]['arg'] = strtr($funHash[$hash]['arg'], $caracs);
                }

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