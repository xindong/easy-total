<?php
ob_clean();


$queries = array_map('unserialize', $this->worker->redis->hGetAll('queries') ?: []);

if (!$queries)
{
    $this->response->header("Location", '/admin/task/list/');
    return;
}

# 排序
uasort($queries, function($a, $b)
{
    return $a['table'] > $b['table'] ? 1 : -1;
});

echo $str = json_encode($queries, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);

$size = strlen($str);
$name = 'easy-total-' .date('Ymd,Hi') . '.json';

$this->response->header("Content-type", "application/octet-stream");
$this->response->header('Content-Disposition', 'attachment; filename="' . $name . '"');
$this->response->header("Content-Length", $size);

return 'noFooter';
