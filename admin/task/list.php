<?php
$queries = $this->worker->redis->hGetAll('queries');


if (!$queries)
{
    echo '<div style="padding:0 15px;"><div class="alert alert-warning">还没有任何任务, <a href="/admin/task/add/">点击这里添加任务</a></div></div>';
    return;
}

foreach ($queries as & $item)
{
    $item = unserialize($item);
}
unset($item);
?>

<div style="padding:0 15px;">
    <div class="row">
        <div class="col-md-12">
            <table class="table table-bordered">
                <thead>
                    <tr>
                    <th>#</th>
                    <th>SQL</th>
                    <th align="center">操作</th>
                    </tr>
                </thead>
                <tbody>
                <?php
                $i = 0;
                foreach ($queries as $query)
                {
                    foreach ($query['sql'] as $sql)
                    {
                        $i++;
                        echo "<tr><td>{$i}</td><td style='line-height:2em;'>$sql</td><td align=\"center\"><button type=\"button\" class=\"btn btn-danger btn-sm\">删除</button></td></tr>";
                    }
                }
                ?>
                </tbody>
            </table>
        </div>
    </div>
</div>