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



require_once __DIR__ . '/../../lib/es/ES.php';
//临时
$query['app'] = 'hsqj';
//过滤
$params = [];
/*
$params = [
    'sid' => [478, 179],
    'cid' => 500034
];
*/
//Common::print_this($query);

$es     = new ES();
$data   = $es->get_result($query, $params);
/*
echo '<pre>';
print_r($data);
echo '</pre>';
*/
?>
<script type="text/javascript" src="/assets/highcharts/highstock.js"></script>
<script src="/assets/highlightjs/highlight.min.js"></script>

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
                    ?>
                </div>
            </div>
        </div>

        <div class="col-md-12">
            <div class="panel panel-success">
                <div class="panel-heading">
                    <h3 class="panel-title">图表数据</h3>
                </div>
                <div class="panel-body">
                    <div id="chart-container"></div>
                </div>
            </div>
        </div>
    </div>
</div>

<script>


    var datas = <?php echo json_encode($data); ?>;

    $(function(){
        $('#chart-container').on('mousemove touchmove touchstart', 'div.chart-box', function (e) {
            var chart,
                point,
                i,
                event;

            for (i = 0; i < Highcharts.charts.length; i = i + 1) {
                chart = Highcharts.charts[i];
                event = chart.pointer.normalize(e.originalEvent); // Find coordinates within the chart
                point = chart.series[0].searchPoint(event, true); // Get the hovered point

                if (point) {
                    point.highlight(e);
                }
            }
        });


        /**
         * Override the reset function, we don't need to hide the tooltips and crosshairs.
         */
        Highcharts.Pointer.prototype.reset = function () {
            return undefined;
        };

        /**
         * Highlight a point by showing tooltip, setting hover state and draw crosshair
         */
        Highcharts.Point.prototype.highlight = function (event) {
            this.onMouseOver(); // Show the hover marker
            this.series.chart.tooltip.refresh(this); // Show the tooltip
            this.series.chart.xAxis[0].drawCrosshair(event, this); // Show the crosshair
        };

        /**
         * Synchronize zooming through the setExtremes event handler.
         */
        function syncExtremes(e) {
            var thisChart = this.chart;

            if (e.trigger !== 'syncExtremes') { // Prevent feedback loop
                Highcharts.each(Highcharts.charts, function (chart) {
                    if (chart !== thisChart) {
                        if (chart.xAxis[0].setExtremes) { // It is null while updating
                            chart.xAxis[0].setExtremes(e.min, e.max, undefined, false, { trigger: 'syncExtremes' });
                        }
                    }
                });
            }
        }

        $.each(datas, function(k, activity){
            //console.log(activity.datasets);
            var _idbox = 'container_'+k;
            if($('#'+_idbox).length > 0) $('#'+_idbox).html('');
            $('#chart-container').append('<div id="'+_idbox+'" class="chart-box"></div>');
            console.log(_idbox);
            $.each(activity.datasets, function (i, dataset) {

                // Add X values
                dataset.data = Highcharts.map(dataset.data, function (val, j) {
                    return [activity.xData[j], val];
                });

                $('<div class="chart">')
                    .appendTo('#'+_idbox)
                    .highcharts({
                        chart: {
                            height: 260,
                            //marginLeft: 40, // Keep all charts left aligned
                            spacingTop: 20,
                            spacingBottom: 20
                        },
                        title: {
                            text: dataset.name,
                            align: 'left',
                            margin: 0,
                            x: 30
                        },
                        credits: {
                            enabled: false
                        },
                        legend: {
                            enabled: false
                        },
                        xAxis: {
                            crosshair: true,
                            events: {
                                setExtremes: syncExtremes
                            },
                            labels: {
                                format: '{value}'
                            }
                        },
                        yAxis: {
                            title: {
                                text: null
                            }
                        },
                        tooltip: {
                            positioner: function () {
                                return {
                                    x: this.chart.chartWidth - this.label.width, // right aligned
                                    y: -1 // align to title
                                };
                            },
                            borderWidth: 0,
                            backgroundColor: 'none',
                            pointFormat: '{point.y}',
                            headerFormat: '',
                            shadow: false,
                            style: {
                                fontSize: '18px'
                            },
                            valueDecimals: dataset.valueDecimals
                        },
                        series: [{
                            data: dataset.data,
                            name: dataset.name,
                            type: dataset.type,
                            color: Highcharts.getOptions().colors[i],
                            fillOpacity: 0.3,
                            tooltip: {
                                valueSuffix: ' ' + (dataset.unit?dataset.unit:'')
                            }
                        }]
                    });
            });
        });
    });
</script>