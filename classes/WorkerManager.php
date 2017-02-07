<?php
class WorkerManager extends MyQEE\Server\WorkerManager
{
    /**
     * 主进程
     *
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

        $uri = implode('/', $uriArr);

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
            $response->status(404);
            $response->end('page not found');
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

        $response->end($html);
    }
}