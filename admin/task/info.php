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
<script type="text/javascript" src="/assets/highcharts/highcharts.js"></script>

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
                echo "<td>{$sql}</td>";
                echo '</tr>';
            }
            ?>
            </table>
        </div>
        <div class="col-md-12">
            <h5>统计曲线</h5>
        </div>
        <div class="col-md-12">
            <table class="table table-bordered">
                <tr>
                    <td>
                        <div id="container" style="height:280px"></div>
                    </td>
                </tr>
            </table>
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
        marginBottom: 60
    },
    credits:{
        enabled: false
    },
    title: {
        text: '数据处理统计曲线'
    },
    xAxis: [{
        categories: <?php echo json_encode(array_keys($total));?>
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
        y: 15,
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

<?php


echo '<pre>';
print_r($query);
echo '</pre>';

