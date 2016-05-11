<script type="text/javascript" src="/assets/highcharts/highstock.js"></script>
<script type="text/javascript" src="/assets/highcharts/highcharts-more.js"></script>
<script type="text/javascript" src="/assets/highcharts/modules/solid-gauge.js"></script>

<?php
if ($this->worker->isSSDB)
{
  $maxSize = 10000;
  $info = [];
  $type = 'SSDB磁盘';


  # 得到ssdb占用的硬盘空间
  $rs = $this->worker->ssdb->info();
  end($rs);
  $data = current($rs);
  $size = 0;
  foreach(explode("\n", trim($data)) as $item)
  {
    $arr = preg_split('#[ ]+#', trim($item));
    if (is_numeric($arr[0]) && isset($arr[2]))
    {
      $size += $arr[2];
    }
  }
  $info['used_memory'] = $size * 1024 * 1024;
  unset($data, $rs, $size);
}
else
{
  $maxSize = 2000;
  $type = 'Redis内存';
  $info = $this->worker->redis->info();
}

$allMemory     = [
  Worker::$serverName => memory_get_usage(true)
];
$allMemoryTotal = $allMemory[Worker::$serverName];

$allMemoryData = $this->worker->redis->hGetAll('server.memory');
if ($allMemoryData)foreach ($allMemoryData as $item)
{
  list($mem, $time, $serv, $wid) = unserialize($item);
  if (Worker::$timed - $time < 60)
  {
    if ($serv != Worker::$serverName || $wid != $this->worker->id)
    {
      $allMemory[$serv] += $mem;
      $allMemoryTotal   += $mem;
    }
  }
}

$stat = $this->server->stats();




# 统计曲线 --------------------------
$time      = time();
$timeBegin = strtotime(date('Y-m-d 00:00:00'));
$useTime   = [];
$pushTime  = [];
$total     = [];
$arrKeys   = [];
for ($i = 0; $i < 1440; $i++)
{
  $timeLimit = $timeBegin + $i * 60;
  $k         = date('H:i', $timeLimit);
  $arrKeys[] = $k;

  if ($timeLimit < $time)
  {
    $total[$k]    = 0;
    $useTime[$k]  = 0;
    $pushTime[$k] = 0;
  }
}
$timeKey       = date('Ymd');
$totalTotalAll = 0;

if ($this->worker->isSSDB)
{
  $keys = $this->worker->ssdb->hlist("counter.total.$timeKey", "counter.total.$timeKey.z", 9999);
}
else
{
  $keys = $this->worker->redis->keys("counter.total.$timeKey.*");
}

$keyLen = strlen('counter.total.');
if ($keys)foreach ($keys as $k)
{
  $tmp            = $this->worker->redis->hGetAll($k) ?: [];
  $totalTotalAll += array_sum($tmp);

  foreach ($tmp as $k1 => $v1)
  {
    $total[$k1] += $v1;
  }

  $tmp = $this->worker->redis->hGetAll('counter.time.'. substr($k, $keyLen)) ?: [];
  foreach ($tmp as $k1 => $v1)
  {
    $useTime[$k1] += $v1 / 1000;
  }

  $tmp = $this->worker->redis->hGetAll('counter.pushtime.'. substr($k, $keyLen)) ?: [];
  foreach ($tmp as $k1 => $v1)
  {
    $pushTime[$k1] += $v1 / 1000;
  }
}
foreach ($useTime as & $item)
{
  $item = number_format($item, 3, '.', '');
}
unset($item);

?>
<style type="text/css">
  .list-group-item {border-color: #bce8f1;}
</style>
<div style="padding:0 15px;">
  <div class="row">
    <div class="col-md-4" style="margin-bottom:15px;">
      <ul class="list-group">
        <li class="list-group-item list-group-item-info">
          <h4 style="margin:2px 0">服务器占用内存</h4>
        </li>
        <li class="list-group-item">
          <div id="container-server" style="height:225px;"></div>
        </li>
      </ul>
    </div>
    <div class="col-md-4" style="margin-bottom:15px;">
      <ul class="list-group">
        <li class="list-group-item list-group-item-info">
          <h4 style="margin:2px 0"><?php echo $type;?>占用</h4>
        </li>
        <li class="list-group-item">
          <div id="container-redis" style="height:225px;"></div>
        </li>
      </ul>
    </div>
    <div class="col-md-4" style="margin-bottom: 15px;">
      <div style="height: 240px">
        <ul class="list-group">
          <li class="list-group-item list-group-item-info">
            <h4 style="margin:2px 0">服务器信息</h4>
          </li>
          <li class="list-group-item">
            <span class="badge" style="background:#5cb85c"><?php echo $this->worker->redis->hLen('queries');?></span>
            <a href="/admin/task/list/">任务数</a>
          </li>
          <li class="list-group-item">
            <span class="badge" style="background:#5bc0de"><?php
              $servers = $this->worker->redis->hGetAll('servers');
              if ($servers)
              {
                $count = 0;
                foreach ($servers as $item)
                {
                  $arr = @json_decode($item, true);
                  if ($arr && time() - $arr['updateTime'] < 120)
                  {
                    $count++;
                  }
                }
                echo $count ?: 1;
              }
              else
              {
                echo '1';
              }
              ?></span>
            集群服务器数
          </li>
          <li class="list-group-item">
            <span class="badge" style="background:#f0ad4e"><?php
              echo number_format($totalTotalAll, 0, '.', ',');
              unset($tmp, $k1);
              ?></span>
            今日处理数据数
          </li>
          <li class="list-group-item">
            <span class="badge" style="background:#f0ad4e"><?php
              $allTotal = $this->worker->redis->hVals('counter') ?: [];
              echo number_format(array_sum($allTotal), 0, '.', ',');
              ?></span>
            累计处理数据总数
          </li>
          <li class="list-group-item">
            <span class="badge" style="background:#f0ad4e"><?php
              echo number_format(FluentServer::getCount(), 0, '.', ',');
            ?></span>
            启动后处理数据数
          </li>
          <li class="list-group-item">
            <span class="badge"><?php echo date('Y-m-d H:i:s', $stat['start_time']);?></span>
            当前服务器启动时间
          </li>
        </ul>

      </div>
    </div>

    <div class="col-md-12">
      <div class="panel panel-warning">
        <div class="panel-heading">
          <h3 class="panel-title">数据处理统计曲线</h3>
        </div>
        <div class="panel-body">
          <div id="container-total"></div>
        </div>
      </div>
    </div>
  </div>
</div>
<script>
  $(function () {
    Highcharts.setOptions({global: {
      useUTC: false,
      timezoneOffset: 8
    }});

    var gaugeOptions = {

      chart: {
        type: 'solidgauge'
      },

      title: null,

      pane: {
        center: ['50%', '85%'],
        size: '140%',
        startAngle: -90,
        endAngle: 90,
        background: {
          backgroundColor: (Highcharts.theme && Highcharts.theme.background2) || '#EEE',
          innerRadius: '60%',
          outerRadius: '100%',
          shape: 'arc'
        }
      },

      tooltip: {
        enabled: false
      },

      // the value axis
      yAxis: {
        stops: [
          [0.1, '#55BF3B'], // green
          [0.6, '#DDDF0D'], // yellow
          [0.8, '#DF5353'] // red
        ],
        lineWidth: 0,
        minorTickInterval: null,
        tickPixelInterval: 400,
        tickWidth: 0,
        title: {
          y: -90
        },
        labels: {
          y: 16
        }
      },

      plotOptions: {
        solidgauge: {
          dataLabels: {
            y: 5,
            borderWidth: 0,
            useHTML: true
          }
        }
      }
    };

    // The speed gauge
    $('#container-redis').highcharts(Highcharts.merge(gaugeOptions, {
      yAxis: {
        min: 0,
        max: <?php echo $maxSize;?>,
        title: {
          text: null
        }
      },

      credits: {
        enabled: false
      },

      series: [{
        name: '内存占用',
        data: [<?php echo number_format($info['used_memory'] / 1024 / 1024, 2, '.', '');?>],
        dataLabels: {
          format: '<div style="text-align:center"><span style="font-size:25px;color:' +
          ((Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black') + '">{y}</span><br/>' +
          '<span style="font-size:12px;color:silver">MB</span></div>'
        },
        tooltip: {
          valueSuffix: ' MB'
        }
      }]
    }));

    // The speed gauge
    $('#container-server').highcharts(Highcharts.merge(gaugeOptions, {
      yAxis: {
        min: 0,
        max: 200,
        title: {
          text: null
        }
      },

      credits: {
        enabled: false
      },

      series: [{
        name: '内存占用',
        data: [<?php echo number_format($allMemoryTotal / 1024 / 1024, 2, '.', '');?>],
        dataLabels: {
          format: '<div style="text-align:center"><span style="font-size:25px;color:' +
          ((Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black') + '">{y}</span><br/>' +
          '<span style="font-size:12px;color:silver">MB</span></div>'
        },
        tooltip: {
          valueSuffix: ' MB'
        }
      }]
    }));


    $('#container-total').highcharts({
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
      xAxis: [{
        categories: <?php echo json_encode($arrKeys);?>,
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

      }, { // Primary yAxis
        labels: {
          format: '{value}ms',
          style: {
            color: Highcharts.getOptions().colors[1]
          }
        },
        title: {
          text: '数据合并消耗时间',
          style: {
            color: Highcharts.getOptions().colors[1]
          }
        },
        opposite: true

      }],
      tooltip: {
        shared: true
      },
      scrollbar: {
        enabled: true,
        liveRedraw: true
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
      }, {
        name: '数据合并消耗时间',
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


});
</script>