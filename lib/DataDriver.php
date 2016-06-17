<?php
class DataDriver
{
    public $config;

    /**
     * @var Redis|mysqli
     */
    protected $connection;

    /**
     * DataDriver constructor.
     */
    public function __construct($config)
    {
        $this->config = $config;
    }

    function __destruct()
    {
        if ($this->connection)
        {
            @$this->connection->close();
        }
    }

    /**
     * 获取统计数据
     *
     * @param $uniqueId
     * @return bool|DataTotalItem
     */
    public function getTotal($uniqueId)
    {
        if (!$this->connection)
        {
            if (!$this->connect())
            {
                return false;
            }
        }

        try
        {
            switch ($this->config['type'])
            {
                case 'mysql':
                    # todo
                    $rs = null;
                    break;

                case 'redis':
                default:
                    $rs = $this->connection->get("total,$uniqueId");
                    break;
            }

            if ($rs)
            {
                $data = @unserialize($rs);
                if (!$data)
                {
                    warn("unserialize total data fail, string: $rs");
                    $data = new DataTotalItem();
                }
            }
            else
            {
                # 没有历史数据
                $data = new DataTotalItem();
            }

            $data->updateTime = time();

            return $data;
        }
        catch(Exception $e)
        {
            warn($e->getMessage());
            $this->connection = null;

            return false;
        }
    }

    /**
     * 保存统计信息
     *
     * @param $uniqueId
     * @param DataTotalItem $total
     * @return bool
     */
    public function saveTotal($uniqueId, DataTotalItem $total)
    {
        if (!$this->connection)
        {
            if (!$this->connect())
            {
                return false;
            }
        }

        try
        {
            switch ($this->config['type'])
            {
                case 'mysql':
                    # todo
                    break;

                case 'redis':
                default:
                    $rs = $this->connection->set("total,$uniqueId", serialize($total));
                    if (false !== $rs)
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
            }

            return true;
        }
        catch (Exception $e)
        {
            $this->connection = null;
            warn($e->getMessage());
            return false;
        }
    }

    /**
     * 更新唯一数
     *
     * 如果更新成功则返回新的唯一数, 如果更新失败则返回失败
     *
     * @param $uniqueId
     * @param $field
     * @param $value
     * @return int|false
     */
    public function saveDist($uniqueId, $field, $value)
    {
        if (!$this->connection)
        {
            if (!$this->connect())
            {
                return false;
            }
        }

        try
        {
            switch ($this->config['type'])
            {
                case 'mysql':
                    $table = str_replace(',', '_', $uniqueId);
                    $c = count($value);
                    $i = 0;
                    $v = '';
                    foreach($value as $k => $tmp)
                    {
                        $i++;
                        if ($i % 1000 === 0 || $i === $c)
                        {
                            $v  .= "('$k')";
                            $sql = "REPLACE INTO `$table` (`id`) VALUES $v";
                            if (!$this->connection->query($sql))
                            {
                                # 失败了
                                return false;
                            }
                            $v   = '';
                        }
                        else
                        {
                            $v .= "('$k'),";
                        }
                    }

                    # 获取唯一数
                    if ($rs = $this->connection->query("select count(1) as `count` from `{$table}`"))
                    {
                        $data  = $rs->fetch_array(MYSQLI_ASSOC);
                        $count = $data[0]['count'];
                        $rs->close();
                    }
                    else
                    {
                        return false;
                    }

                    break;

                case 'redis':
                default:
                    $c = count($value);
                    for ($i = 0; $i < $c; $i += 1000)
                    {
                        # 每1000条设置1次
                        if (false === $this->connection->hMSet("dist,$uniqueId,$field", array_slice($value, $i, 1000, true)))
                        {
                            # 失败了
                            return false;
                        }
                    }

                    # 获取新长度
                    $count = $this->connection->hLen("dist,$uniqueId,$field");
                    break;
            }

            return $count;
        }
        catch (Exception $e)
        {
            $this->connection = null;
            warn($e->getMessage());
            return false;
        }
    }

    protected function connect()
    {
        try
        {
            switch ($this->config['type'])
            {
                case 'mysql':
                    list($host, $port) = explode(':', $this->config['link']);
                    $arr = parse_url($this->config['link']);
                    $this->connection = new mysqli($arr['host'], $arr['port'] ?: 3306, $arr['pass'], trim($arr['path']));

                    if ($this->connection->connect_errno)
                    {
                        throw new Exception($this->connection->connect_error, $this->connection->connect_errno);
                    }
                    break;

                case 'redis':
                default:
                    if (is_array($this->config['link']))
                    {
                        $this->connection = new RedisCluster(null, $this->config['link']);
                    }
                    else
                    {
                        list($host, $port) = explode(':', $this->config['link']);
                        $this->connection = new Redis();
                        $this->connection->connect($host, $port);
                    }

                    break;
            }

            return true;
        }
        catch (Exception $e)
        {
            warn($e->getMessage());
            $this->connection = null;
            return false;
        }
    }
}