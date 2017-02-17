<?php
/**
 * @var Swoole\Http\Request $request
 */
$key = $request->get['key'];

if (!$key)
{
    echo '<div style="padding:0 15px;"><div class="alert alert-warning">缺少参数</div></div>';
    return;
}

if (!$this->worker->redis)
{
    echo '<div style="padding:0 15px;"><div class="alert alert-warning">redis服务器没有启动</div></div>';
    return;
}

$query = $this->worker->redis->hget('queries', $key);

if ($query)$query = unserialize($query);

if (!$query)
{
    echo '<div style="padding:0 15px;"><div class="alert alert-warning">指定的查询不存在</div></div>';
    return;
}
?>
<script type="text/javascript" src="/assets/highcharts/highstock.js"></script>
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
}

.hljs {
    background: transparent;
}
</style>

<div style="padding:0 15px;margin-top:-15px">
    <div class="row">
        <div class="col-md-12">
            <div class="pull-left" style="padding:10px 0 0 0">
                <a href="/admin/">管理首页</a> <i style="font-size:11px;width:1em" class="glyphicon glyphicon-menu-right"></i>
                <a href="/admin/task/list/">任务列表</a> <i style="font-size:11px;width:1em" class="glyphicon glyphicon-menu-right"></i>
                任务管理
            </div>
            <div class="pull-right" style="margin:5px 0 10px 0;"><a href="/admin/task/list/"><button class="btn btn-primary btn-sm">返回列表</button></a></div>
        </div>

        <div class="col-md-12">
            <div class="panel panel-success">
                <div class="panel-heading">
                    <h3 class="panel-title">“<?php echo $query['name'];?>” 基本信息</h3>
                </div>
                <div class="panel-body">
                    <?php
                    $stats      = $query['use'] ? 'ok' : 'pause';
                    $statsColor = $query['use'] ? '#d43f3a' : '#eea236';

                    echo "<strong style='font-size:13px;'>运行状态:</strong> &nbsp;<i style=\"width:1em;font-size:9px;color:".$statsColor."\" class=\"glyphicon glyphicon-".$stats."\"></i>\n";

                    echo '<span style="width:50px;display:inline-block"></span>';
                    echo "<strong style='font-size:13px;'>输出:</strong>";
                    foreach ($query['saveAs'] as $k => $v)
                    {
                        echo " &nbsp;<span data-toggle=\"tooltip\" data-placement=\"bottom\" title='按 {$k} 时间分组输出到 ". (is_array($v)?$v[0]:$v) ."' style='cursor:default'><span class='label label-success' style='border-radius:3px 0 0 3px'>".(is_array($v)?$v[0]:$v)."</span><span style='border-radius:0 3px 3px 0' class='label label-info'>$k</span></span>";
                    }

                    if ($query['groupBy'])
                    {
                        echo '<strong style="font-size:13px;margin-left:60px;">字段分组:</strong> &nbsp;<span class="label label-danger">'. implode('</span><span class="label label-danger" style="margin-left: 5px;">', $query['groupBy']) .'</span>';
                    }

                    echo '<span style="width:50px;display:inline-block"></span>';
                    echo "<strong style='font-size:13px;'>创建时间:</strong> &nbsp;<span class='label label-warning'>". date('Y-m-d H:i:s', $query['createTime']) ."</span>\n";
                    if ($query['editTime'])
                    {
                        echo '<span style="width:50px;display:inline-block"></span>';
                        echo "<strong style='font-size:13px'>修改时间</strong> &nbsp;<span class='label label-warning'>". date('Y-m-d H:i:s', $query['editTime']) ."</span>\n";
                    }

                    echo '<br /><br />';
                    echo '<strong style="font-size:13px;">SQL语句:</strong> ';
                    echo '<div style="display:none" class="edit-sql">';
                    echo '<form class="task-edit-form" action="/api/task/update" method="post">';
                    echo '<input type="hidden" name="key" value="'. $query['key'] .'" />';
                    echo '<textarea name="sql" class="form-control" rows="3" style="margin-bottom: 6px;font-family: Menlo,Monaco,Consolas,monospace;">'. $query['sql'] .'</textarea>';
                    echo '<button type="submit" class="btn btn-info">保存修改</button> ';
                    echo '<button type="button" class="btn btn-default edit-sql-btn">取消</button>';
                    echo '</form>';
                    echo '<div class="alert alert-danger" style="margin:8px 0 0 0;">注意: 若修改 from, where, group by 任意一个条件, 则会使用<strong>新的数据统计序列</strong>, 这意味着之前的统计数据将对新SQL统计无效（历史数据将在24小时内清理掉), 若你不希望这样请添加新的SQL统计, 修改其它参数则不影响, 如果你增加了一个新 group time, 或 select 字段等, 则会在保存后开始统计, 之前的数据也不会有（除非已经存在）</div>';
                    echo '</div>';
                    echo '<pre class="highlight show-sql">';
                    echo '<code class="mysql">'. $query['sql'] .'</code>';
                    echo '</pre>';
                    echo '<div><button type="button" style="margin-top:6px;padding:3px 10px;" class="btn btn-info edit-sql-btn">修改</button></div>';

                    ?>
                </div>
            </div>
        </div>
        <script type="text/javascript">
            $('.edit-sql-btn').on('click', function()
            {
                var obj = $('.edit-sql');
                var display = obj.css('display');
                if (display == 'none')
                {
                    $('.show-sql').css('display', 'none');
                    $(this).css('display', 'none');
                    obj.css('display', '').focus();
                }
                else
                {
                    obj.css('display', 'none');
                    $('.show-sql').css('display', '');
                    $('.edit-sql-btn').css('display', '');
                }
            });
        </script>

        <div class="col-md-12">
            <div class="panel panel-warning">
                <div class="panel-heading">
                    <h3 class="panel-title">任务处理统计</h3>
                </div>
                <div class="panel-body">
                    <div id="container" style="height:300px"></div>
                </div>
            </div>
        </div>

        <div class="col-md-12">
            <div class="panel panel-danger">
                <div class="panel-heading">
                    <h3 class="panel-title">输出字段设置</h3>
                </div>
                <div class="panel-body">
                    <?php
                    $typeMap = [
                        'value' => '赋值',
                        'sum'   => '求和',
                        'count' => '总数',
                        'max'   => '最大值',
                        'min'   => '最小值',
                        'first' => '第一个值',
                        'last'  => '最后一个值',
                        'dist'  => '唯一数',
                        'list'  => '列出数据',
                        'avg'   => '求平均值',
                        'func'  => '函数运算',
                        'exclude'   => '排除字段',
                        'listcount' => '列出数据数',
                    ];

                    # 根据时间戳得到当前的时间key
//                        $totalKey     = "total,$key,$app,{$query['groupTime']['limit']}{$query['groupTime']['type']}";

                    echo '<table class="table table-bordered table-striped table-hover" style="margin:0"><thead><tr style="white-space:nowrap">
                        <th>输出字段名</th>
                        <th>数据源字段名</th>
                        <th>运算类型</th>
                        <th width="40">数值</th>
                        <th style="text-align:center" width="100">操作</th>
                    </tr></thead>';
                    echo '<tr>
                        <td>_id</td>
                        <td>数据主键值</td>
                        <td>系统</td>
                        <td></td>
                        <td></td>
                        </tr>';
                    echo '<tr>
                        <td>_group</td>
                        <td>时间分组值</td>
                        <td>系统</td>
                        <td></td>
                        <td></td>
                        </tr>';
                    if ($query['allField'])
                    {
                        echo '<tr>
                        <td>*</td>
                        <td>全部字段赋值</td>
                        <td>赋值</td>
                        <td>-</td>
                        <td style="text-align:center"></td>
                        </tr>';
                    }
                    foreach ($query['fields'] as $k => $v)
                    {
                        echo '<tr>
                        <td>'. $k .'</td>
                        <td>'. $v['field'] .'</td>
                        <td>'. $v['type'] .' ('. $typeMap[$v['type']] .')</td>
                        <td style="text-align:center;">'. ($v['type'] !== 'value' ? '<button class="btn btn-success btn-xs" type="button" data-toggle="tooltip" title="查看统计内容"><i class="glyphicon glyphicon-eye-open" style="width:1em"></i></button>': '') .'</td>
                        <td style="text-align:center"><button class="btn btn-info btn-xs" type="button">修改</button> <button class="btn btn-danger btn-xs" type="button">删除</button></td>
                        </tr>';
                    }

                    echo '</table>';
                    ?>
                </div>
            </div>

        </div>

        <div class="col-md-12">
            <h4>统计中间值</h4>
            <table class="table table-bordered table-striped table-hover">
                <thead>
                <tr style="white-space: nowrap">
                    <th>键</th>
                    <th>值</th>
                    <th width="40">操作</th>
                </tr>
                </thead>
                <?php
                if ($this->worker->isSSDB)
                {
                    $data = $this->worker->ssdb->scan("total,{$key},", "total,{$key},z", 50);
                }
                else
                {
                    $it      = null;
                    $arrKeys = $this->worker->redis->scan($it, "total,{$key},*", 50);
                    $data    = [];
                    foreach($arrKeys as $strKey)
                    {
                        $data[$strKey] = $this->worker->redis->get($strKey);
                    }
                }

                if ($data)
                {
                    $len = strlen("total,{$key},");
                    foreach ($data as $k => $v)
                    {
                        $v = unserialize($v) ?: [];
                ?>
                <tr>
                    <td style="white-space: nowrap"><?php echo substr($k, $len);?></td>
                    <td><pre class="highlight"><code class="json"><?php echo htmlentities(json_encode($v, JSON_UNESCAPED_UNICODE));?></code></pre></td>
                    <td style="text-align:center;"><button class="btn btn-xs btn-danger">删</button></td>
                </tr>
                <?php
                    }
                }
                else
                {
                    echo '<tr><td colspan="3">没有相关数据</td></tr>';
                }
                ?>
            </table>

            <nav>
                <ul class="pager">
                    <li class="disabled"><a href="#">上一页</a></li>
                    <li><a href="#">下一页</a></li>
                </ul>
            </nav>
        </div>


        <div class="col-md-12">
            <h4>配置数据</h4>
            <?php
            echo '<pre style="background: #fcfcfc"><code class="json">';
            echo htmlspecialchars(json_encode($query, JSON_PRETTY_PRINT|JSON_UNESCAPED_UNICODE));
            echo '</code></pre>';
            ?>
            <h4>序列配置</h4>
            <?php
            echo '<pre style="background: #fcfcfc"><code class="json">';
            echo htmlspecialchars(json_encode(unserialize($this->worker->redis->hget('series', $query['seriesKey'])), JSON_PRETTY_PRINT|JSON_UNESCAPED_UNICODE));
            echo '</code></pre>';
            ?>
        </div>

    </div>
</div>

<?php

$time      = time() - 60;
$timeBegin = strtotime(date('Y-m-d 00:00:00'));
$useTime   = [];
$total     = [];
$arrKeys   = [];
for ($i = 0; $i < 1440 ; $i++)
{
    $timeLimit = $timeBegin + $i * 60;
    $k         = date('H:i', $timeLimit);
    $arrKeys[] = $k;

    if ($timeLimit < $time)
    {
        $useTime[$k]  = 0;
        $total[$k]    = 0;
    }
}
$seriesKey = $query['seriesKey'];
$timeKey   = date('Ymd');
$useTime   = array_merge($useTime, $this->worker->redis->hGetAll("counter.time.$timeKey.$seriesKey") ?: []);
$total     = array_merge($total, $this->worker->redis->hGetAll("counter.total.$timeKey.$seriesKey") ?: []);

$useTime = array_map(function($v)
{
    # 转成毫秒
    return number_format($v / 1000, 3, '.', '');
}, $useTime);
?>

<script type="text/javascript">
Highcharts.setOptions({global: {
    timezoneOffset: 8
}});


$('#container').highcharts({
    chart: {
        zoomType: 'x',
        marginBottom: 80
    },
    credits:{
        enabled: false
    },
    title: {
        text: null
    },
    scrollbar: {
        enabled: true,
        liveRedraw: true
    },
    xAxis: [{
        categories: <?php echo json_encode(array_keys($total));?>,
        range: 100
    }],
    yAxis: [{ // Secondary yAxis
        gridLineWidth: 0,
        labels: {
            style: {
                color: Highcharts.getOptions().colors[0]
            }
        },
        title: {
            text: '处理数据数量',
            style: {
                color: Highcharts.getOptions().colors[0]
            }
        }

    }, {
        labels: {
            format: '{value}ms',
            style: {
                color: Highcharts.getOptions().colors[1]
            }
        },
        title: {
            text: '耗时',
            style: {
                color: Highcharts.getOptions().colors[1]
            }
        },
        opposite: true

    }],
    tooltip: {
        shared: true
    },
    legend: {
        align: 'center',
        y: 5,
        verticalAlign: 'bottom',
        backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF'
    },
    series: [{
        name: '处理数据数量',
        type: 'spline',
        yAxis: 0,
        data: <?php echo json_encode(array_values($total), JSON_NUMERIC_CHECK);?>,
        marker: {
            enabled: false
        }
    }, {
        name: '处理数据耗时',
        type: 'spline',
        data: <?php echo json_encode(array_values($useTime), JSON_NUMERIC_CHECK);?>,
        yAxis: 1,
        dashStyle: 'shortdot',
        tooltip: {
            valueSuffix: 'ms'
        },
        marker: {
            enabled: false
        }
    }]
});
</script>


<script type="text/javascript">
    $('.task-edit-form').on('submit', function(e)
    {
        e.stopPropagation();
        e.preventDefault();
        var formData = {
            key : this.elements.key.value,
            sql : this.elements.sql.value
        };

        if (formData.sql == '')
        {
            alert('任务SQL不能空');
            return false;
        }

        $.ajax({
            url: '/api/task/update',
            data: formData,
            type: 'post',
            dataType: 'json',
            success: function(data, status, xhr)
            {
                if (data.status == 'error')
                {
                    alert(data.message || '更新失败');
                    return;
                }
                alert('更新成功');
                window.location.reload();
            },
            error: function(xhr, status, err)
            {
                alert('请求服务器失败');
            }
        });

        return false;
    });
</script>


<script>hljs.initHighlightingOnLoad();</script>