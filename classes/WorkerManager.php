<?php
class WorkerManager extends MyQEE\Server\WorkerManager
{
    public function onStart()
    {
        $this->worker = EtServer::$workers['EasyTotal'];
    }

    /**
     * @param \Swoole\Http\Request $request
     * @param \Swoole\Http\Response $response
     * @return mixed
     */
    public function onRequest($request, $response)
    {
        $this->request  = $request;
        $this->response = $response;

        $uri    = trim($request->server['request_uri'], ' /');
        $uriArr = explode('/', $uri);
        array_shift($uriArr);

        $this->admin(implode('/', $uriArr));

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
    public function onMessage($server, $frame)
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
    public function onOpen($server, $request)
    {
        $this->debug("server: handshake success with fd{$request->fd}");
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
        $this->debug($file);

        if (!is_file($file))
        {
            $this->response->status(404);
            $this->response->end('page not found');
            return;
        }

        ob_start();
        include __DIR__ .'/../admin/_header.php';
        if (!$this->worker->redis)
        {
            echo '<div style="padding:0 15px;"><div class="alert alert-danger" role="alert">redis服务器没有启动</div></div>';
            $rs = null;
        }
        else
        {
            $rs = include $file;
        }

        if ($rs !== 'noFooter')
        {
            include __DIR__ . '/../admin/_footer.php';
        }
        $html = ob_get_clean();

        $this->response->end($html);
    }
}