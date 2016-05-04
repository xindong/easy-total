<?php
ob_clean();

$this->response->header("Content-type", "text/json");

$rs = [];
if (!$this->request->post['data'])
{
    $rs['status'] = 'error';
    $rs['message'] = '没有获取到导入的数据';
}
else
{
    $arr = @json_decode(trim($this->request->post['data']), true);
    if (!$arr)
    {
        $rs['status']  = 'error';
        $rs['message'] = '解析数据失败, 无法导入';
        echo trim($this->request->post['data']);
    }
    else
    {
        $rs['status'] = 'ok';

        foreach ($arr as $key => $value)
        {
            $this->worker->redis->hset('queries', $key, serialize($value));
        }
    }
}

echo json_encode($rs, JSON_UNESCAPED_UNICODE);

return 'noFooter';
