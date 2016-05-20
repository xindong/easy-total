<?php
/**
 * Fluent日志处理核心类
 *
 * 配置根目录 `$config['log']['fluent'] = 'tcp://127.0.0.1:24224/'` 后
 * 使用 `self::log('myapp.test.debug', $_SERVER)` 默认就可以调用本方法
 *
 *
 *      Fluent::instance('tcp://127.0.0.1:24224/')->push('xd.game.test', ["test"=>"hello"]);
 *
 *      Fluent::instance('unix:///full/path/to/my/socket.sock')->push('xd.game.test', ["test"=>"hello"]);
 *
 *
 * @see        https://github.com/fluent/fluent-logger-php
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Core
 * @package    Classes
 * @copyright  Copyright (c) 2008-2016 myqee.com
 * @license    http://www.myqee.com/license.html
 */
class FluentClient
{
    const CONNECTION_TIMEOUT = 3;
    const SOCKET_TIMEOUT     = 3;
    const MAX_WRITE_RETRY    = 5;

    /* 1000 means 0.001 sec */
    const USLEEP_WAIT = 1000;

    /**
     * 是否开启ACK
     *
     * @var bool
     */
    const REQUIRE_ACK_RESPONSE = true;

    /**
     * backoff strategies: default usleep
     *
     * attempts | wait
     * 1        | 0.003 sec
     * 2        | 0.009 sec
     * 3        | 0.027 sec
     * 4        | 0.081 sec
     * 5        | 0.243 sec
     **/
    const BACKOFF_TYPE_EXPONENTIAL = 0x01;
    const BACKOFF_TYPE_USLEEP      = 0x02;

    /**
     * 服务器
     *
     * 例如 `tcp://127.0.0.1:24224`
     *
     * @var string
     */
    protected $transport;

    /* @var resource */
    protected $socket;

    protected $is_http = false;

    protected $data = [];

    protected $options = array
    (
        'socket_timeout'       => self::SOCKET_TIMEOUT,
        'connection_timeout'   => self::CONNECTION_TIMEOUT,
        'backoff_mode'         => self::BACKOFF_TYPE_USLEEP,
        'backoff_base'         => 3,
        'usleep_wait'          => self::USLEEP_WAIT,
        'persistent'           => true,
        'retry_socket'         => true,
        'max_write_retry'      => self::MAX_WRITE_RETRY,
        'require_ack_response' => self::REQUIRE_ACK_RESPONSE,
        'max_buffer_length'    => 1000,
    );

    /**
     * @var FluentClient
     */
    protected static $instance = array();

    function __construct($server, array $option = array())
    {
        $this->transport = $server;

        if (($pos = strpos($server, '://')) !== false)
        {
            $protocol = substr($server, 0, $pos);

            if (!in_array($protocol, array('tcp', 'udp', 'unix', 'http')))
            {
                throw new Exception("transport `{$protocol}` does not support");
            }

            if ($protocol === 'http')
            {
                # 使用HTTP推送
                $this->is_http = true;
                $this->transport = rtrim($this->transport, '/ ');
            }
        }
        else
        {
            throw new Exception("fluent config error");
        }

        if ($option)
        {
            $this->options = array_merge($this->options, $option);
        }
    }

    /**
     * destruct objects and socket.
     *
     * @return void
     */
    public function __destruct()
    {
        if ($this->data)
        {
            # 把遗留的数据全部推送完毕
            foreach (array_keys($this->data) as $tag)
            {
                $this->push($tag);
            }
        }

        if (!$this->get_option('persistent', false) && is_resource($this->socket))
        {
            @fclose($this->socket);
        }
    }

    /**
     * 返回Fluent处理对象
     *
     * @return FluentClient
     */
    public static function instance($server)
    {
        if (!isset(FluentClient::$instance[$server]))
        {
            FluentClient::$instance[$server] = new FluentClient($server);
        }

        return FluentClient::$instance[$server];
    }

    /**
     * 添加数据，添加完毕后并不直接推送
     *
     * 当开启ack后，推荐先批量 add 后再push，当超过 max_buffer_length 后会自动推送到服务器
     *
     *      $fluent = new Fluent('tcp://127.0.0.1:24224/');
     *      $fluent->add('debug.test1', array('a' => 1));
     *      $fluent->add('debug.test2', array('a' => 2));
     *      $fluent->add('debug.test1', array('a' => 1));
     *
     *      var_dump($fluent->push('debug.test1'));
     *      var_dump($fluent->push('debug.test2'));
     *
     * @param string $tag tag内容
     * @param array $data 数据内容
     * @param int $time 标记日志的时间戳，不设置就是当前时间
     */
    public function add($tag, array $data, $time = null)
    {
        $this->_add($tag, $data, $time);

        if (count($this->data[$tag]) >= $this->options['max_buffer_length'])
        {
            return $this->push($tag);
        }

        return true;
    }

    /**
     * 清除数据
     *
     * @param null $tag
     * @return $this
     */
    public function clear($tag = null)
    {
        if ($tag)
        {
            unset($this->data[$tag]);
        }
        else
        {
            $this->data = [];
        }

        return $this;
    }

    protected function _add($tag, $data, $time)
    {
        if ($this->is_http)
        {
            if (!isset($data['time']))
            {
                $data['time'] = $time ? $time : time();
            }
            $this->data[$tag][] = $data;
        }
        else
        {
            $this->data[$tag][] = array($time ? $time : time(), $data);
        }
    }

    /**
     * 推送数据到服务器
     *
     * @param string $tag tag内容
     * @param array $data 数据内容
     * @param int $time 标记日志的时间戳，不设置就是当前时间
     * @return bool
     * @throws Exception
     */
    public function push($tag, $data = null, $time = null)
    {
        if ($data)
        {
            $this->_add($tag, $data, $time);
        }

        if (!isset($this->data[$tag]) || !$this->data[$tag])return true;

        if ($this->is_http)
        {
            $rs = $this->push_with_http($tag, $time);
        }
        else
        {
            $rs = $this->push_with_socket($tag);
        }

        if ($rs)
        {
            $this->clear($tag);
        }

        return $rs;
    }

    protected function push_with_http($tag, $time)
    {
        $packed  = self::json_encode($this->data[$tag]);
        $url     = $this->transport .'/'. $tag .'?time='. ($time ? $time : time()) .'&json='. urlencode($packed);

        $ret = file_get_contents($url);

        return ($ret !== false && $ret === '');
    }

    protected function push_with_socket($tag)
    {
        $data = $this->data[$tag];

        if ($ack = $this->get_option('require_ack_response'))
        {
            $ack_key = 'a'. (microtime(1) * 10000);
            $buffer = self::json_encode(array($tag, $data, array('chunk' => $ack_key)))."//==\n";
        }
        else
        {
            $ack_key = null;
            $buffer = self::json_encode(array($tag, $data))."\n";
        }

        return $this->push_by_buffer($tag, $buffer, $ack_key);
    }

    /**
     * 直接提交数据
     *
     * @param $tag
     * @param $buffer
     * @param null $ack_key
     * @return bool
     */
    public function push_by_buffer($tag, $buffer, $ack_key = null)
    {
        $packed = $buffer;
        $length = strlen($buffer);
        $retry  = $written = 0;

        try
        {
            $this->reconnect();
        }
        catch (Exception $e)
        {
            $this->close();
            $this->process_error($tag, $buffer, $e->getMessage());

            return false;
        }

        try
        {
            // PHP socket looks weired. we have to check the implementation.
            while ($written < $length)
            {
                $nwrite = $this->write($buffer);

                if ($nwrite === false)
                {
                    // could not write messages to the socket.
                    // e.g) Resource temporarily unavailable
                    throw new Exception("could not write message");
                }
                else if ($nwrite === '')
                {
                    // sometimes fwrite returns null string.
                    // probably connection aborted.
                    throw new Exception("connection aborted");
                }
                else if ($nwrite === 0)
                {
                    if (!$this->get_option("retry_socket", true))
                    {
                        $this->process_error($tag, $buffer, "could not send entities");

                        return false;
                    }

                    if ($retry > $this->get_option("max_write_retry", self::MAX_WRITE_RETRY))
                    {
                        throw new Exception("failed fwrite retry: retry count exceeds limit.");
                    }

                    $errors = error_get_last();
                    if ($errors)
                    {
                        if (isset($errors['message']) && strpos($errors['message'], 'errno=32 ') !== false)
                        {
                            /* breaking pipes: we have to close socket manually */
                            $this->close();
                            $this->reconnect();

                            # 断开后重新连上后从头开始写，避免出现 incoming chunk is broken 的错误问题
                            $written = 0;
                            $buffer = $packed;
                            continue;
                        }
                        else if (isset($errors['message']) && strpos($errors['message'], 'errno=11 ') !== false)
                        {
                            // we can ignore EAGAIN message. just retry.
                        }
                        else
                        {
                            error_log("unhandled error detected. please report this issue to http://github.com/fluent/fluent-logger-php/issues: ". var_export($errors, true));
                        }
                    }

                    if ($this->get_option('backoff_mode', self::BACKOFF_TYPE_EXPONENTIAL) == self::BACKOFF_TYPE_EXPONENTIAL)
                    {
                        $this->backoff_exponential(3, $retry);
                    }
                    else
                    {
                        usleep($this->get_option("usleep_wait", self::USLEEP_WAIT));
                    }
                    $retry++;
                    continue;
                }

                $written += $nwrite;
                $buffer   = substr($packed, $written);
            }

            if ($ack_key)
            {
                $rs = @fread($this->socket, 1024);
                if ($rs)
                {
                    $rs = @json_decode($rs, true);
                    if ($rs && isset($rs['ack']))
                    {
                        if ($rs['ack'] !== $ack_key)
                        {
                            warn('ack in response and chunk id in sent data are different. rs: '. $rs['ack']. 'send: '. $ack_key);
                            return false;
                        }
                        else
                        {
                            return true;
                        }
                    }
                    else
                    {
                        warn('error response data: '. $rs);
                        return false;
                    }
                }
                else
                {
                    warn('error response data:'. $rs);
                    return false;
                }
            }
        }
        catch (Exception $e)
        {
            $this->close();
            warn($e->getMessage());

            return false;
        }

        return true;
    }


    /**
     * write data
     *
     * @param string $data
     * @return mixed integer|false
     */
    protected function write($buffer)
    {
        // We handle fwrite error on postImpl block. ignore error message here.
        return @fwrite($this->socket, $buffer);
    }

    /**
     * create a connection to specified fluentd
     *
     * @throws \Exception
     */
    protected function connect()
    {
        $connect_options = STREAM_CLIENT_CONNECT;
        if ($this->get_option("persistent", false))
        {
            $connect_options |= STREAM_CLIENT_PERSISTENT;
        }

        // could not suppress warning without ini setting.
        // for now, we use error control operators.
        $socket = @stream_socket_client($this->transport, $errno, $errstr, $this->get_option("connection_timeout", self::CONNECTION_TIMEOUT), $connect_options);

        if (!$socket)
        {
            $errors = error_get_last();
            throw new Exception($errors['message']);
        }

        // set read / write timeout.
        stream_set_timeout($socket, $this->get_option("socket_timeout", self::SOCKET_TIMEOUT));

        $this->socket = $socket;
    }

    /**
     * create a connection if Fluent Logger hasn't a socket connection.
     *
     * @return void
     */
    protected function reconnect()
    {
        if (!is_resource($this->socket))
        {
            $this->connect();
        }
    }

    /**
     * close the socket
     *
     * @return void
     */
    public function close()
    {
        if (is_resource($this->socket))
        {
            fclose($this->socket);
        }

        $this->socket = null;
    }

    /**
     * get specified option's value
     *
     * @param      $key
     * @param null $default
     * @return mixed
     */
    protected function get_option($key, $default = null)
    {
        $result = $default;
        if (isset($this->options[$key]))
        {
            $result = $this->options[$key];
        }

        return $result;
    }

    /**
     * backoff exponential sleep
     *
     * @param $base int
     * @param $attempt int
     */
    protected function backoff_exponential($base, $attempt)
    {
        usleep(pow($base, $attempt) * 1000);
    }

    /**
     * 处理错误
     *
     * @param $tag
     * @param $buffer
     * @param $error
     */
    protected function process_error($tag, $buffer, $error)
    {
        error_log(sprintf("%s %s: %s", $error, $tag, $buffer));
    }

    protected static function json_encode(array $data)
    {
        try
        {
            // 解决使用 JSON_UNESCAPED_UNICODE 偶尔会出现编码问题导致json报错
            return defined('JSON_UNESCAPED_UNICODE') ? json_encode($data, JSON_UNESCAPED_UNICODE) : json_encode($data);
        }
        catch (Exception $e)
        {
            return json_encode($data);
        }
    }
}