<?php
/**
 * Class FluentInForward for Swoole
 *
 * 可以直接在 swoole 服务器中接受处理 fluent 的 forward 协议数据
 * @see http://docs.fluentd.org/articles/in_forward
 */
class FluentInForward
{
    /**
     * @var \Swoole\Server
     */
    protected $server;

    /**
     * 当程序需要终止时临时导出的文件路径（确保数据安全）
     *
     * @var string
     */
    protected $dumpFile = '';

    /**
     * 记录数据的数组
     *
     * @var array
     */
    protected $buffer = [];

    /**
     * 记录数据的最后时间
     *
     * @var array
     */
    protected $bufferTime = [];

    /**
     * 数据包是否JSON格式
     *
     * @var array
     */
    protected $bufferIsJSON = [];

    /**
     * 需要处理的事件列表
     *
     *  事件key    | 回调函数参数                | 说明
     *  ----------|---------------------------|--------------------
     *  checkTag  | & $tag, & $extra          | 检查tag是否需要处理数据, 返回 true 表示需要处理
     *  each      | $tag, $records, & $extra  | 逐条处理数据, 抛出异常则全部退出
     *  ack       | status                    | 发送完ACK后回调
     *
     * @var array
     */
    protected $event = [];

    /**
     * 任务id
     *
     * @var int
     */
    protected $tickId = 0;
    
    /**
     * 当前时间, 会一直更新
     *
     * @var int
     */
    public static $time;

    /**
     * 单个数据包最大长度
     *
     * 单位字节（默认52428800 即 50MB）
     *
     * @var int
     */
    public static $packMaxLength = 52428800;

    /**
     * 包协议分隔符
     *
     * @var string
     */
    protected static $packKey = '';

    public function __construct($server)
    {
        $this->server = $server;

        # 包数据的key
        self::$packKey = chr(146).chr(206);

        # 设置默认回调
        $this->event['each'] = function($tag, $records, & $extra)
        {
        };
        $this->event['checkTag'] = function(& $tag, & $extra)
        {
            return true;
        };
        $this->event['ack'] = function($status, & $extra)
        {
            return true;
        };

        # 每10分钟处理1次
        $this->tickId = swoole_timer_tick(1000 * 600, function()
        {
            # 清理老数据
            if ($this->buffer)
            {
                self::$time = time();
                foreach ($this->buffer as $fd)
                {
                    if (self::$time - $this->bufferTime[$fd] > 300)
                    {
                        # 超过5分钟没有更新数据, 则移除
                        $this->info('clear expired data length: '. strlen($this->buffer[$fd]));

                        $this->clearBuffer($fd);
                    }
                }
            }
        });
    }

    function __destruct()
    {
        # 销毁清理定时器
        swoole_timer_clear($this->tickId);
    }

    /**
     * 接受到数据时回调
     *
     * @param Swoole\Server $server
     * @param $fd
     * @param $fromId
     * @param $data
     * @return bool
     */
    public function onReceive($server, $fd, $fromId, $data)
    {
        self::$time = time();

        if (!isset($this->buffer[$fd]))
        {
            # 包头
            switch (ord($data[0]))
            {
                case 0x92:      # MsgPack 的3数组
                case 0x93:      # MsgPack 的4数组
                    $this->bufferIsJSON[$fd] = false;
                    break;

                case 0x5b;      # json 格式的 [ 字符
                    $this->bufferIsJSON[$fd] = true;
                    break;

                default:
                    $this->warn("accept unknown data length: ". strlen($data). ', head ascii is: '. ord($data[0]));
                    return true;
            }

            $this->buffer[$fd]     = $data;
            $this->bufferTime[$fd] = self::$time;
        }
        else
        {
            $this->buffer[$fd]    .= $data;
            $this->bufferTime[$fd] = self::$time;
        }

        # 解开数据
        if ($this->bufferIsJSON[$fd])
        {
            $arr       = $this->unpackByJson($fd);
            $isMsgPack = false;
        }
        else
        {
            $arr       = $this->unpackByMsgPack($fd);
            $isMsgPack = true;
        }

        if (!$arr || !is_array($arr))
        {
            if (($len = strlen(($this->buffer[$fd]))) > self::$packMaxLength)
            {
                # 超过50MB
                $this->clearBuffer($fd);

                $this->warn("pack data is too long: {$len}byte. now close client.");

                # 关闭连接
                $this->closeConnect($fd);

                return false;
            }

            return true;
        }

        # 处理数据
        $this->execute($fd, $fromId, $arr, $isMsgPack);

        if (!isset($this->buffer[$fd]))return true;

        # 处理粘包的数据
        while(true)
        {
            # 删除上一个引用地址
            unset($arr);

            if ($this->bufferIsJSON[$fd])
            {
                $arr       = $this->unpackByJson($fd);
                $isMsgPack = false;
            }
            else
            {
                $arr       = $this->unpackByMsgPack($fd);
                $isMsgPack = true;
            }

            if (!$arr || !is_array($arr))
            {
                break;
            }

            # 处理数据
            $this->execute($fd, $fromId, $arr, $isMsgPack);

            if (!isset($this->buffer[$fd]))
            {
                break;
            }
        }

        return true;
    }

    /**
     * 当连接时回调
     *
     * @param Swoole\Server $server
     * @param $fd
     * @param $fromId
     */
    public function onConnect($server, $fd, $fromId)
    {
        if (isset($this->buffer[$fd]))
        {
            $this->clearBuffer($fd);
        }
    }

    /**
     * 当连接关闭时回调
     *
     * @param Swoole\Server $server
     * @param $fd
     * @param $fromId
     */
    public function onClose($server, $fd, $fromId)
    {
        $this->server = null;
        if (isset($this->buffer[$fd]))
        {
            $this->clearBuffer($fd);
        }
    }

    /**
     * 事件回调
     *
     * @param $event
     * @param $callback
     * @return $this
     */
    public function on($event, $callback)
    {
        $this->event[$event] = $callback;

        return $this;
    }

    /**
     * 清理所有buffer数据
     */
    public function cleanAll()
    {
        $this->buffer       = [];
        $this->bufferIsJSON = [];
        $this->bufferTime   = [];
    }

    /**
     * 处理数据
     *
     * @param $fd
     * @param $fromId
     * @param $arr
     * @param $isMsgPack
     * @return bool
     */
    protected function execute($fd, $fromId, & $arr, $isMsgPack)
    {
        $tag = $arr[0];
        if (!$tag || !is_string($tag))
        {
            $this->warn('error data, not found tag');

            # 把客户端关闭了
            $this->closeConnect($fd);
            return false;
        }

        # 查看连接信息
        $info = $this->server->connection_info($fd, $fromId);
        if (false === $info)
        {
            # 连接已经关闭
            $this->warn("connection is closed. tag: {$tag}");
            $this->clearBuffer($fd);
            return false;
        }
        elseif (self::$time - $info['last_time'] > 30)
        {
            # 最后发送的时间距离现在已经超过 30 秒, 直接不处理, 避免 ack 确认超时的风险
            $this->closeConnect($fd);
            $this->info("connection wait timeout: " . (self::$time - $info['last_time']) ."s. tag: $tag");
            return false;
        }
        unset($info);

        # 是否需要再解析（Fluentd 会把数据通过 buffer 字符串直接传过来）
        $delayParseRecords = $isMsgPack && is_string($arr[1]);

        if ($delayParseRecords || is_array($arr[1]))
        {
            # 多条数据
            # [tag, [[time,record], [time,record], ...], option]
            $option  = $arr[2] ?: [];
            $records = $arr[1];
        }
        else
        {
            # 单条数据
            # [tag, time, record, option]
            $option  = $arr[3] ?: [];
            $records = [[$arr[1], $arr[2]]];
        }

        if ($option && $option['chunk'])
        {
            $ackData = ['ack' => $option['chunk']];
        }
        else
        {
            $ackData = null;
        }

        # 是否解开数据
        $extra = [];

        # 是否需要处理数据
        if (call_user_func_array($this->event['checkTag'], [&$tag, &$extra]))
        {
            $parse = true;
        }
        else
        {
            $parse = false;
        }

        if ($parse)
        {
            try
            {
                if ($delayParseRecords)
                {
                    # 解析数据
                    $this->parseRecords($records);
                }

                # 处理数据
                $rs = call_user_func_array($this->event['each'], [$tag, $records, &$extra]);
                if (!$rs)
                {
                    # 返回错误
                    $this->closeConnect($fd);

                    return false;
                }
            }
            catch (Exception $e)
            {
                # 执行失败
                $this->closeConnect($fd);

                return false;
            }
        }

        if ($ackData)
        {
            # ACK 确认
            if ($isMsgPack)
            {
                $tmp    = msgpack_pack($ackData);
                $isSend = $this->server->send($fd, $tmp);
            }
            else
            {
                $tmp    = json_encode($ackData);
                $isSend = $this->server->send($fd, $tmp);
            }

            if (IS_DEBUG && !$isSend)
            {
                $this->debug("send ack data fail. fd: $fd, data: $tmp");
            }
        }
        else
        {
            $isSend = true;
        }

        # 回调
        call_user_func_array($this->event['ack'], [$isSend, $extra]);

        return true;
    }

    protected function unpackByJson($fd)
    {
        # JSON 格式数据结尾
        $arr = @json_decode($this->buffer[$fd], true);
        if (!$arr)
        {
            # 处理粘包的可能
            # ["tag", 1234567890, {"key":"value1"}]["tag", 1234567890, {"key":"value2"}]
            if (($pos = strpos($this->buffer[$fd], '[')) > 0)
            {
                # 移除开头不是 [ 的内容
                $this->warn("error json data: ". substr($this->buffer[$fd], 0, $pos));
                $this->buffer[$fd] = substr($this->buffer[$fd], $pos);
            }

            $len = strlen($this->buffer[$fd]);
            $tmp = '';
            for ($i = 0; $i < $len; $i++)
            {
                $tmp .= $this->buffer[$fd][$i];
                if ($this->buffer[$fd][$i] === ']')
                {
                    $arr = @json_decode($tmp, true);
                    if (is_array($arr) && $arr)
                    {
                        $this->buffer[$fd] = substr($this->buffer[$fd], $i + 1);

                        return $arr;
                    }
                }
            }

            return false;
        }
        else
        {
            $this->clearBuffer($fd);

            $countArr = count($arr);
            if ($countArr < 3)
            {
                $this->warn("unknown data, array count mush be 3 or 4, unpack data count is: $countArr");
                $this->closeConnect($fd);

                return false;
            }

            return $arr;
        }
    }

    protected function unpackByMsgPack($fd)
    {
        $arr = @msgpack_unpack($this->buffer[$fd]);

        if (!$arr || !is_array($arr))
        {
            return false;
        }
        else
        {
            if (count($arr) < 2)
            {
                return false;
            }

            $this->clearBuffer($fd);
            return $arr;
        }
    }

    /**
     * 关闭连接
     *
     * @param $fd
     */
    public function closeConnect($fd)
    {
        $this->clearBuffer($fd);
        $this->server->close($fd);
    }

    public function clearBuffer($fd)
    {
        unset($this->buffer[$fd]);
        unset($this->bufferTime[$fd]);
        unset($this->bufferIsJSON[$fd]);
    }

    protected function parseRecords(& $recordsData)
    {
        if (is_string($recordsData))
        {
            # 解析里面的数据
            $tmpArr = [];
            $arr    = explode(self::$packKey, $recordsData);
            $len    = count($arr);
            $str    = '';

            for ($i = 1; $i < $len; $i++)
            {
                $str .= self::$packKey . $arr[$i];

                $tmpRecord = @msgpack_unpack($str);
                if (false !== $tmpRecord && is_array($tmpRecord))
                {
                    $tmpArr[] = $tmpRecord;

                    # 重置临时字符串
                    $str = '';
                }
            }

            $recordsData = $tmpArr;
        }
    }

    /**
     * 错误信息, 可替换成自己的log函数
     *
     * @param string $info
     */
    protected function warn($info)
    {
        EtServer::$instance->log($info, null, 'warn', '[31m');
    }

    /**
     * 输出信息, 可替换成自己的log函数
     *
     * @param string $info
     */
    protected function info($info)
    {
        EtServer::$instance->log($info, null, 'info', '[33m');
    }

    /**
     * 调试信息, 可替换成自己的log函数
     *
     * @param string $info
     */
    protected function debug($info)
    {
        EtServer::$instance->log($info, null, 'debug', '[34m');
    }
}
