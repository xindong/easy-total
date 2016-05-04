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

.hljs {
    background: transparent;
}
</style>

<div style="padding:0 15px;margin-top:-15px">
    <div class="row">
        <div class="col-md-12">
            <div style="text-align:right;margin:5px 0 10px 0;"><a href="/admin/task/list/"><button class="btn btn-primary btn-sm">返回列表</button></a></div>
        </div>
        <div class="col-md-12">
            <table class="table table-bordered table-striped">
            <thead>
                <tr style="white-space:nowrap;">
                    <th width="60" style="text-align:center">运行中</th>
                    <th>名称</th>
                    <th>输出表</th>
                    <th>SQL</th>
                </tr>
            </thead>
            <?php
            foreach ($query['sql'] as $table => $sql)
            {
                $setting    = $query['setting'][$table] ?: [];
                $stats      = $query['use'] ? 'ok' : 'pause';
                $statsColor = $query['use'] ? '#d43f3a' : '#eea236';

                echo '<tr>';
                echo '<td style="text-align:center"><i style="width:1em;font-size:9px;color:'.$statsColor.'" class="glyphicon glyphicon-'.$stats.'"></i></td>';
                echo "<td>{$setting['name']}</td>";
                echo "<td>{$table}</td>";
                echo "<td><pre class='highlight'><code class=\"mysql\">{$sql}</code></pre></td>";
                echo '</tr>';
            }
            ?>
            </table>
        </div>
        <div class="col-md-12">
            <h4>运行记录监控</h4>
        </div>
        <div class="col-md-12">
            <table class="table table-bordered">
                <tr>
                    <td>
                        <div id="container" style="height:300px"></div>
                    </td>
                </tr>
            </table>
        </div>

        <div class="col-md-12">
            <h4>统计中间值</h4>
            <table class="table table-bordered table-striped">
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
                    $data = $this->worker->ssdb->scan("total,{$key}_", "total,{$key}_z", 50);
                }
                else
                {
                    $it      = null;
                    $arrKeys = $this->redis->scan($it, "total,{$key}_*", 50);
                    $data    = [];
                    foreach($arrKeys as $strKey)
                    {
                        $data[$strKey] = $this->redis->get($strKey);
                    }
                }

                if ($data)
                {
                    $len = strlen("total,{$key}_");
                    foreach ($data as $key => $item)
                    {
                        $item = unserialize($item) ?: [];
                ?>
                <tr>
                    <td style="white-space: nowrap"><?php echo substr($key, $len);?></td>
                    <td><pre class="highlight"><code class="json"><?php echo htmlentities(json_encode($item, JSON_UNESCAPED_UNICODE));?></code></pre></td>
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
$timeKey = date('Y-m-d');
$useTime = array_merge($useTime, $this->worker->redis->hGetAll("counter.time.$timeKey.$key") ?: []);
$total   = array_merge($total, $this->worker->redis->hGetAll("counter.total.$timeKey.$key") ?: []);

$total   = array_map('intval', $total);
$useTime = array_map('intval', $useTime);
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
        text: '数据处理统计曲线',
        y: 20
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
            text: '累计请求数',
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
            text: '累计耗时',
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
        name: '累计请求数',
        type: 'spline',
        yAxis: 0,
        data: <?php echo json_encode(array_values($total));?>,
        marker: {
            enabled: false
        }
    }, {
        name: '累计耗时',
        type: 'spline',
        data: <?php echo json_encode(array_values($useTime));?>,
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