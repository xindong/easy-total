<?php
/**
 * @var Swoole\Http\Response $response
 */
ob_clean();


$queries = array_map('unserialize', $this->worker->redis->hGetAll('queries') ?: []);

if (!$queries)
{
    $response->header("Location", '/admin/task/list/');
    return;
}

# 排序
uasort($queries, function($a, $b)
{
    return $a['table'] > $b['table'] ? 1 : -1;
});

echo $str = json_encode(['version' => '1.0', 'queries' => $queries], JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);

$size = strlen($str);
$name = 'easy-total-' .date('Ymd,Hi') . '.json';

$response->header("Content-type", "application/octet-stream");
$response->header('Content-Disposition', 'attachment; filename="' . $name . '"');
$response->header("Content-Length", $size);

return 'noFooter';
