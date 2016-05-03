<?php
$queries = $this->worker->redis->hGetAll('queries');


if (!$queries)
{
    echo '<div style="padding:0 15px;"><div class="alert alert-warning">还没有任何任务, <a href="/admin/task/add/">点击这里添加任务</a></div></div>';
    return;
}

?>
<div style="padding:0 15px;">
    <div class="row">
        <div class="col-md-12">
            <div class="text-right" style="margin:-10px 0 10px 0"><a href="/admin/task/add/"><button type="button" class="btn btn-primary btn-sm">添加新任务</button></a></div>
            <table class="table table-bordered table-striped">
                <thead>
                    <tr style="white-space:nowrap">
                    <th style="text-align:center" width="50">#</th>
                    <th>名称</th>
                    <th>SQL</th>
                    <th width="120" style="text-align:center">输出</th>
                    <th width="45" style="text-align:center">状态</th>
                    <th width="130" style="text-align:center">创建时间</th>
                    <th width="140" style="text-align:center">操作</th>
                    </tr>
                </thead>
                <tbody>
                <?php
                $i = 0;
                foreach ($queries as $query)
                {
                    $query = unserialize($query);
                    foreach ($query['sql'] as $saveAs => $sql)
                    {
                        $i++;
                        $setting    = $query['setting'][$saveAs] ?: [];
                        $stats      = $query['use'] ? 'ok' : 'pause';
                        $statsColor = $query['use'] ? '#d43f3a' : '#eea236';
                        echo "<tr>
<td style=\"text-align:center\">{$i}</td>
<td>{$setting['name']}</td>
<td style='font-size:12px;'>$sql</td>
<td style=\"text-align:center;white-space:nowrap\">{$saveAs}</td>
<td style=\"text-align:center;\"><i style='font-size:9px;color:{$statsColor}' class=\"glyphicon glyphicon-{$stats}\"></i></td>
<td style='text-align:center;font-size:12px;padding-top:11px'>".($setting['time'] ? date('Y-m-d H:i:s', $setting['time']) : '-')."</td>
<td align=\"center\">
<a href=\"/admin/task/info/?key={$query['key']}\"><button type=\"button\" class=\"btn btn-info btn-xs\">查看</button></a>
<a href=\"/admin/task/pause/\"><button type=\"button\" class=\"btn btn-warning btn-xs\">暂停</button></a>
<button data-key=\"{$query['key']}\" data-save-as=\"{$saveAs}\" type=\"button\" class=\"btn btn-danger btn-xs task-delete\">删除</button>
</td></tr>";
                    }
                }
                ?>
                </tbody>
            </table>
        </div>
    </div>
</div>

<script type="text/javascript">
    $('.task-delete').on('click', function()
    {
        var $this  = $(this);
        var key    = $this.data('key');
        var saveAs = $this.data('saveAs');
        if (confirm('确定要删除?'))
        {
            $.ajax({
                url: '/api/task/remove',
                data: {
                    key: key,
                    table: saveAs
                },
                type: 'post',
                dataType: 'json',
                success: function(data, status, xhr)
                {
                    if (data.status == 'error')
                    {
                        alert(data.message || '删除失败');
                        return;
                    }
                    alert('删除成功');
                    window.location.reload();
                },
                error: function(xhr, status, err)
                {
                    alert('请求服务器失败');
                }
            });
        }
    });
</script>