<?php
$queries = array_map('unserialize', $this->worker->redis->hGetAll('queries') ?: []);

if (!$queries)
{
    echo '<div style="padding:0 15px;"><div class="alert alert-warning">还没有任何任务, <a href="/admin/task/add/">点击这里添加任务</a></div></div>';
    return;
}

uasort($queries, function($a, $b)
{
    return $a['table'] > $b['table'] ? 1 : -1;
});

?>

<link rel="stylesheet" href="/assets/highlightjs/tomorrow.min.css">
<script src="/assets/highlightjs/highlight.min.js"></script>
<style type="text/css">
.highlight{
    border:none;
    padding:0;
    margin:0;
    background-color: transparent;
}
.highlight code {
    padding:0;
    margin:0;
}

.hljs {
    background: transparent;
}
</style>

<div style="padding:0 15px;">
    <div class="row">
        <div class="col-md-12">
            <div class="pull-right" style="margin:-10px 0 10px 0">
                <input type="file" class="task-import" style="cursor:pointer;width: 70px;height:30px;position: absolute;opacity:0.01" />
                <button type="button" class="btn btn-info btn-sm">导入任务</button>
                <a href="/admin/task/export/"><button type="button" class="btn btn-success btn-sm">导出任务</button></a>
                <a href="/admin/task/add/"><button type="button" class="btn btn-primary btn-sm">添加新任务</button></a>
            </div>
            <table class="table table-bordered table-striped table-hover">
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
                    $i++;
                    $stats      = $query['use'] ? 'ok' : 'pause';
                    $statsText  = $query['use'] ? '运行中' : '暂停';
                    $statsColor = $query['use'] ? '#d43f3a' : '#eea236';
                    $saveAs     = implode(',', $query['saveAs']);

                    if ($query['deleteTime'] > 0)
                    {
                        # 已经删除了

                        $stats      = 'remove-circle';
                        $statsText  = '已移除';
                        $statsColor = '#d9534f';
                    }
                    echo "<tr>
<td style=\"text-align:center\">{$i}</td>
<td>{$query['name']}</td>
<td><pre class='highlight'><code class=\"mysql\">{$query['sql']}</code></pre></td>
<td style=\"text-align:center;white-space:nowrap\">{$saveAs}</td>
<td style=\"text-align:center;\"><i style='font-size:9px;color:{$statsColor}' data-toggle=\"tooltip\" title=\"$statsText\" class=\"glyphicon glyphicon-{$stats}\"></i></td>
<td style='text-align:center;font-size:12px;padding-top:11px'>".date('Y-m-d H:i:s', $query['createTime'])."</td>
<td align=\"center\">
<a href=\"/admin/task/info/?key={$query['key']}\"><button type=\"button\" class=\"btn btn-info btn-xs\">管理</button></a>
".
($query['use'] ?
"<button data-key=\"{$query['key']}\" type=\"button\" data-type=\"pause\" class=\"btn btn-warning btn-xs task-edit-status\">暂停</button>"
:
"<button data-key=\"{$query['key']}\" type=\"button\" data-type=\"start\" class=\"btn btn-warning btn-xs task-edit-status\">启用</button>"
). " ". ($query['deleteTime'] > 0 ?
"<button data-key=\"{$query['key']}\" data-type=\"restore\" type=\"button\" class=\"btn btn-danger btn-xs task-edit-status\">恢复</button>"
:
"<button data-key=\"{$query['key']}\" data-type=\"remove\" type=\"button\" class=\"btn btn-danger btn-xs task-edit-status\">删除</button>"
).
"</td></tr>";
                }
                ?>
                </tbody>
            </table>
        </div>
    </div>
</div>

<script type="text/javascript">
    $('.task-import').on('change', function()
    {
        if (window.File && window.FileReader && window.FileList && window.Blob)
        {
            var reader   = new FileReader();
            var fileList = this.files;

            reader.onload = function(e)
            {
                if (confirm('将会覆盖现有相同的配置, 是否继续?'))
                {
                    var data = e.target.result;
                    if (!data)
                    {
                        alert('读取了一个空文件');
                        return false;
                    }

                    $.ajax({
                        url: '/admin/task/import/',
                        data: {
                            'data': data
                        },
                        method: 'post',
                        dataType: 'json',
                        success: function(data, status, xhr)
                        {
                            if (data.status == 'error')
                            {
                                alert(data.message || '添加失败');
                                return;
                            }
                            alert('导入成功');
                            window.location.reload();
                        },
                        error: function(xhr, status, err)
                        {
                            alert('请求服务器失败');
                        }
                    });
                }
                else
                {
                    alert('您放弃了导入操作');
                }
            };

            reader.readAsText(fileList[0]);
        }
        else
        {
            alert('你的浏览器版本太旧, 不支持');
        }

        // 重置
        this.value = '';
    });

    $('.task-edit-status').on('click', function()
    {
        var $this  = $(this);
        var key    = $this.data('key');
        var saveAs = $this.data('saveAs');
        var type   = $this.data('type');
        var typeMap = {
            pause    : '你确定要暂停此任务?暂停后任务将不再处理后续数据',
            remove   : '你确定要删除此任务?删除后相关的数据将在24小时内清除',
            start    : '是否要开启当前任务?',
            restore  : '当前任务已经删除会在24小时内移除并清理, 此操作将取消移除并恢复启用, 是否继续?'
        };

        if (!typeMap[type])return;

        if (confirm(typeMap[type]))
        {
            $.ajax({
                url: '/api/task/'+ type,
                data: {
                    key: key
                },
                method: 'post',
                dataType: 'json',
                success: function(data, status, xhr)
                {
                    if (data.status == 'error')
                    {
                        alert(data.message || '操作失败');
                        return;
                    }
                    alert('操作成功');
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

<script>hljs.initHighlightingOnLoad();</script>