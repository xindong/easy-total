<?php
$key = $this->request->get['key'];

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
                        echo " &nbsp;<span data-toggle=\"tooltip\" data-placement=\"bottom\" title='按 {$k} 时间分组输出到 {$v}' style='cursor:default'><span class='label label-success' style='border-radius:3px 0 0 3px'>$v</span><span style='border-radius:0 3px 3px 0' class='label label-info'>$k</span></span>";
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
                        echo "<strong style='font-size:13px'>修改时间</strong> &nbsp;<span class='label label-warning'>". date('Y-m-d H:i:s', $setting['editTime']) ."</span>\n";
                    }

                    echo '<br /><br /><strong style="font-size:13px;">SQL语句:</strong> ';
                    echo '<pre class="highlight"><code class="mysql">'. $query['sql'] .'</code></pre>';

                    ?>
                </div>
            </div>
        </div>

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
            <div class="panel panel-info">
                <div class="panel-heading">
                    <div class="pull-right"><button type="button" style="margin:-4px -4px 0 0" class="btn btn-info btn-xs"<?php if (($queryCount = count($query['saveAs'])) > 1)echo ' data-toggle="tooltip" data-placement="left" title="注意, 此任务有'.$queryCount.'个子任务, 一旦修改其它任务也会受到影响, 若不想影响另外任务你可以重新创建后移除旧任务"';?>>修改</button></div>
                    <h3 class="panel-title">Where条件</h3>
                </div>
                <div class="panel-body">
                    <?php echo $query['where']['$sql'] ?: '无';?>
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
                    $timeLimitKey = Worker::getTimeKey(time(), $query['groupTime']['type'], $query['groupTime']['limit']);
//                        $totalKey     = "total,$key,$app,{$query['groupTime']['limit']}{$query['groupTime']['type']}";

                    echo '<table class="table table-bordered table-striped table-hover" style="margin:0"><thead><tr style="white-space:nowrap">
                        <th>输出字段名</th>
                        <th>数据源字段名</th>
                        <th>运算类型</th>
                        <th>当前数值</th>
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
                        <td>'. ($v['type'] === 'dist' ? '<div class="pull-right"><button class="btn btn-success btn-xs" type="button" data-toggle="tooltip" title="查看序列内容"><i class="glyphicon glyphicon-eye-open" style="width:1em"></i></button></div>': '') .'</td>
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
                    $arrKeys = $this->redis->scan($it, "total,{$key},*", 50);
                    $data    = [];
                    foreach($arrKeys as $strKey)
                    {
                        $data[$strKey] = $this->redis->get($strKey);
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
            echo htmlspecialchars(json_encode($query, JSON_PRETTY_PRINT));
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
        $useTime[$k] = 0;
        $total[$k]   = 0;
    }
}
$timeKey = date('Ymd');
$useTime = array_merge($useTime, $this->worker->redis->hGetAll("counter.time.$timeKey.$key") ?: []);
$total   = array_merge($total, $this->worker->redis->hGetAll("counter.total.$timeKey.$key") ?: []);

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
            text: '处理数据量',
            style: {
                color: Highcharts.getOptions().colors[0]
            }
        }

    }, { // Primary yAxis
        labels: {
            format: '{value}ms',
            style: {
                color: Highcharts.getOptions().colors[1]
            }
        },
        title: {
            text: '消耗时间',
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
        name: '处理数据量',
        type: 'spline',
        yAxis: 0,
        data: <?php echo json_encode(array_values($total), JSON_NUMERIC_CHECK);?>,
        marker: {
            enabled: false
        }
    }, {
        name: '消耗时间',
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


<script>hljs.initHighlightingOnLoad();</script>