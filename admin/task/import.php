<?php

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
    elseif (!isset($arr['version']) || !$arr['version'] || version_compare($arr['version'], '1.0', '<'))
    {
        $rs['status']  = 'error';
        $rs['message'] = '版本太低, 无法导入, 请用文本编辑器打开, 逐个SQL添加';
    }
    else
    {
        $rs['status'] = 'ok';

        foreach ($arr['queries'] as $key => $option)
        {
            if (!$option['sql'])
            {
                $rs['errorItem'][$key] = $option;
                continue;
            }

            # 解析SQL
            $newOption = SQL::parseSql($option['sql']);
            if (!$newOption)
            {
                $rs['errorItem'][$key] = $option;
                continue;
            }

            # 以下参数沿用旧的
            $newOption['key']        = $option['key'];
            $newOption['name']       = $option['name'];
            $newOption['use']        = $option['use'];
            $newOption['createTime'] = $option['createTime'];
            $newOption['editTime']   = $option['editTime'];
            $newOption['start']      = $option['start'];
            $newOption['end']        = $option['end'];

            $this->worker->redis->hset('queries', $key, serialize($newOption));
            $this->createSeriesByQueryOption($newOption);
        }
    }

    # 重新加载任务
    $this->notifyAllWorker('task.reload');
}

ob_clean();

echo json_encode($rs, JSON_UNESCAPED_UNICODE);

return 'noFooter';
