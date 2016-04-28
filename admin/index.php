<script type="text/javascript" src="/assets/highcharts/highcharts.js"></script>
<script type="text/javascript" src="/assets/highcharts/highcharts-more.js"></script>
<script type="text/javascript" src="/assets/highcharts/modules/solid-gauge.js"></script>

<?php
if ($this->worker->isSSDB)
{
  $info = [];
  $type = 'SSDB磁盘';
  $info['used_memory'] = $this->worker->redis->dbSize();
}
else
{
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
?>
<div style="padding:0 15px;">
  <div class="row">
    <div class="col-md-4">
      <div id="container-server" style="height: 250px;"></div>
    </div>
    <div class="col-md-4">
      <div id="container-redis" style="height: 250px;"></div>
    </div>
    <div class="col-md-4">
      <div style="height: 240px">
        <ul class="list-group">
          <li class="list-group-item list-group-item-warning">
            <h4 style="margin:2px 0">服务器信息</h4>
          </li>
          <li class="list-group-item">
            <span class="badge">123,223,333</span>
            数据执行请求数
          </li>
          <li class="list-group-item">
            <span class="badge"><?php echo date('Y-m-d H:i:s', $stat['start_time']);?></span>
            服务器启动时间
          </li>
        </ul>

      </div>
    </div>
  </div>
</div>
<script>
  $(function () {

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
        max: 500,
        title: {
          text: '<?php echo $type;?>占用'
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
          text: '服务占用内存'
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

    // Bring life to the dials
//    setTimeout(function () {
//      // Speed
//      var chart = $('#container-speed').highcharts(),
//        point,
//        newVal,
//        inc;
//
//      if (chart) {
//        point = chart.series[0].points[0];
//        inc = Math.round((Math.random() - 0.5) * 100);
//        newVal = point.y + inc;
//
//        if (newVal < 0 || newVal > 200) {
//          newVal = point.y - inc;
//        }
//
//        point.update(newVal);
//      }
//    }, 2000);


  });
</script>