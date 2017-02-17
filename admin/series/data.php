<?php
/**
 * @var Swoole\Http\Request $request
 */
    try
    {
        $seriesKey = $request->get['key'];

        if (!$seriesKey)
        {
            throw new Exception("缺失序列参数!");
        }

        if ($this->worker->isSSDB)
        {
            $serieDetail = unserialize($this->worker->ssdb->hget('series', $seriesKey));
        }
        else
        {
            $serieDetail = unserialize($this->worker->redis->hget('series', $seriesKey));
        }

        if (!$serieDetail)
        {
            throw new Exception("此序列信息不存在!");
        }

        $data = array('status' => true);
    }
    catch (Exception $e)
    {
        $data = array('status' => false, 'message' => $e->getMessage());
    }
?>

<script type="text/javascript" src="/assets/highcharts/highstock.js"></script>
<link rel="stylesheet" href="/assets/highlightjs/tomorrow.min.css">
<script src="/assets/highlightjs/highlight.min.js"></script>

<div style="padding:0 15px;margin-top:-15px">
    <div class="row">
        <div class="col-md-12">
            <div class="pull-left" style="padding:10px 0 0 0">
                <a href="/admin/">管理首页</a> <i style="font-size:11px;width:1em" class="glyphicon glyphicon-menu-right"></i>
                <a href="/admin/series/list/">任务列表</a> <i style="font-size:11px;width:1em" class="glyphicon glyphicon-menu-right"></i>
                统计数据
            </div>
        </div>
    </div>
<?php
    if (!$data['status'])
    {
?>
        <p class="bg-danger"><?php echo $data['message'];?></p>
</div>
<?php
        exit;
    }
?>

    <div class="panel panel-success">
        <div class="panel-heading">
            <h3 class="panel-title">dist统计</h3>
        </div>
        <div class="panel-body">
            <div id="container_dist" style="height:300px"></div>
        </div>
    </div>

    <div class="panel panel-success">
        <div class="panel-heading">
            <h3 class="panel-title">total统计</h3>
        </div>
        <div class="panel-body">
            <div id="container_total" style="height:300px"></div>
        </div>
    </div>

    <div class="panel panel-success">
        <div class="panel-heading">
            <h3 class="panel-title">join统计</h3>
        </div>
        <div class="panel-body">
            <div id="container_join" style="height:300px"></div>
        </div>
    </div>
</div>

<script>
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
        }, {
            name: '汇总合并耗时',
            type: 'spline',
            data: <?php echo json_encode(array_values($pushTime), JSON_NUMERIC_CHECK);?>,
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