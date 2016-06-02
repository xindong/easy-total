<?php

require_once __DIR__ . '/autoload.php';


class ES
{
    /**
     * 请求地址
     * @var array $hosts
     */
    private $hosts = [
        'http://123.56.9.194:9200'
    ];

    /**
     * @var \Elasticsearch\Client $client
     */
    private $client;

    /**
     * @var array $config
     */
    private $config = [];

    public function __construct($config = [])
    {
        $this->config = $config;

        if(array_key_exists('host', $this->config))
        {
            $this->hosts = (is_array($this->config['host']))?$this->config['host']:[$this->config['host']];
        }

    }

    public function client()
    {
        if(!$this->client)
        {
            $this->client = Elasticsearch\ClientBuilder::create()
                ->setHosts($this->hosts)
                ->build();
        }
        return $this->client;
    }

    public function delete_index($index)
    {
        $client = $this->client();
        $deleteParams = [
            'index' => $index
        ];
        $ret = $client->indices()->delete($deleteParams);
        if(array_key_exists('acknowledged', $ret) && $ret['acknowledged'])
        {
            return true;
        }
        else
        {
            $error_info = json_encode($ret);
            throw new Exception($error_info);
        }
    }

    public function execute($index, $type, $body)
    {
        $client = $this->client();
        $ret    = $client->search([
            'index' => $index,
            'type'  => $type,
            'body'  => $body
        ]);

        if(array_key_exists('error', $ret))
        {
            echo('es return error : '.$ret['error']);
        }

        return $ret;
    }


    /**
     * 统计的日志
     * @param $index string
     * 统计日期 e.g. 2016-03-31
     * @param $type
     * 查询语句体
     * @param $body
     *  [
     *      g1 => [
     *          g1_v1 => [],
     *          g1_v2 => [
     *              g1-1 => [
     *                  g1-1_v1 => [...]
     *              ]
     *          ]
     *      ],
     *      g2 => []
     *      ...
     *  ]
     * @return array
     */
    public function aggregate($index, $type, $body)
    {
        $ret    = $this->execute($index, $type, $body);
        $aggs   = $ret['aggregations'];
        $fields = self::get_aggs_value_fields($body);
        $data   = self::handle_data($aggs, $fields);
        return $data;
    }


    /**
     * es 返回 aggs 数据, 可以指定某个具体的 aggs
     * @param $infos array
     * 统计值定义的字段名
     * @param $fields array
     * @return array
     */
    public static function handle_data($infos, $fields)
    {
        $ret = [];
        if(is_array($infos) && array_key_exists('buckets', $infos))
        {
            foreach($infos['buckets'] as $info)
            {
                $key  = $info['key'];
                $keys = array_keys($info);

                if(array_intersect($fields, $keys))
                {
                    foreach(array_intersect($fields, $keys) as $field)
                    {
                        $ret[$key][$field] = $info[$field]['value'];
                        unset($info[$field]);
                    }
                }

                if(array_diff($fields, $keys))
                {
                    $_ret = self::handle_data($info, $fields);
                    $ret[$key] = (array_key_exists($key, $ret)?$ret[$key]:[]) + $_ret;
                }
            }
        }
        else
        {
            foreach($infos as $_k => $_info)
            {
                if(is_array($_info))
                {
                    $_ret = self::handle_data($_info, $fields);
                    $ret[$_k] = (array_key_exists($_k, $ret)?$ret[$_k]:[]) + $_ret;
                }
            }
        }
        return $ret;
    }


    public static function get_aggs_value_fields($body)
    {
        $fields = [];
        foreach($body as $k => $item)
        {
            if(is_array($item))
            {
                if(array_key_exists('aggs', $item))
                {
                    foreach($item['aggs'] as $_k => $_item)
                    {
                        if(array_key_exists('aggs', $_item))
                        {
                            $_fields = self::get_aggs_value_fields($item);
                            $fields  = $fields + $_fields;
                        }
                        else
                        {
                            array_push($fields, $_k);
                        }
                    }
                }
                else
                {
                    $_fields = self::get_aggs_value_fields($item);
                    $fields  = $fields + $_fields;
                }
            }
        }
        return $fields;
    }




    public function get_result($config, $params = [])
    {
        //过滤
        /*
        $params = [
            'sid' => [478, 179],
            'cid' => 500034
        ];
        */

        //$params = $_GET;

        $must   = [];
        foreach($config['groupBy'] as $field)
        {
            if(array_key_exists($field, $params) && $params[$field] != '')
            {
                $v = $params[$field];
                if(is_array($v))
                {
                    $must[] = [
                        'terms' => [$field => array_map('intval', $v)],
                    ];
                }
                else
                {
                    $must[]= [
                        'term' => [$field => (int)$v],
                    ];
                }
            }
        }

        $body = [];
        if($must)
        {
            $body['query']  = [
                'filtered' => [
                    'filter' => [
                        'bool' => [
                            'must' => $must
                        ]
                    ]
                ]
            ];
        }

        $aggs   = [];
        foreach($config['fields'] as $name => $info)
        {
            $t = $info['type'];
            if($t == 'value') continue;
            $aggs[$name] = [
                'sum' => [
                    'field' => $name
                ]
            ];
        }

        //Common::print_this($aggs);

        $body['aggs'] = [
            '_group' => [
                'terms' => [
                    'field' => '_group',
                    'size'  => 25,
                    'order' => [
                        '_term' => 'desc'
                    ],
                ],
                'aggs' => $aggs
            ]
        ];
        $body['size'] = 0;

        $save_as = $config['saveAs'];

        $indexes = [];
        $time    = time();
        foreach($save_as as $k => $info)
        {
            if(is_array($info))
            {
                $table = $info[0];
                $dinfo = $info[2];
                $rep   = [];
                foreach($dinfo as $d)
                {
                    $rep[] = date($this->caracs[$d], $time);
                }
                $table = str_replace($dinfo, $rep, $table);
                //Common::print_this($table);
                $indexes[$k] = $table;
            }
            else
            {
                $indexes[$k] = $info;
            }
        }

        //Common::print_this($indexes);
        $ret    = [];
        $app    = $config['app'];
        foreach($indexes as $k => $index)
        {
            // $index        = $config['save_as']['1d'];
            $_index         = $app.'-'.$index;
            $_data          = $this->aggregate($_index, '', $body);

            //Common::print_this($_data);
            $ret[$_index]   = $this->trans($_data);
        }

        //Common::print_this($ret);

        $ret2 = [];
        foreach($ret as $key => $item)
        {
            foreach($item as $k => $v)
            {
                $v = array_reverse($v, true);
                if(!$ret2[$key]['xData'])
                {
                    $ret2[$key]['xData'] = array_keys($v);
                }
                $ret2[$key]['datasets'][$k] = [
                    'name' => $k,
                    'data' => array_values($v),
                    'type' => 'spline',
                    'valueDecimals' => 0
                ];
            }
        }

        //Common::print_this($ret2);

        return $ret2;
    }

    public function trans($data)
    {
        $ret = [];
        if(array_key_exists('_group', $data))
        {
            foreach($data['_group'] as $d =>  $item)
            {
                foreach($item as $k => $v)
                {
                    $ret[$k][$d] = $v;
                }
            }
        }
        return $ret;
    }


    public $caracs = [
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
}
