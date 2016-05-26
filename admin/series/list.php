<?php
if ($this->worker->isSSDB)
{

}else{
    $queris = array_map('unserialize', $this->worker->redis->hGetAll('queries') ?: []);

    $series = [];
    if ($queris)foreach ($queris as $query)
    {
        $series[] = unserialize($this->worker->redis->hget('series', $query['seriesKey']));
    }
}
?>

<div style="padding:0 15px;margin-top:-15px">
    <div class="row">
        <div class="col-md-12">
            <div class="pull-left" style="padding:10px 0 0 0">
                <a href="/admin/">管理首页</a> <i style="font-size:11px;width:1em" class="glyphicon glyphicon-menu-right"></i>
                统计序列管理
            </div>
        </div>
    </div>

    <table class="table table-bordered table-striped table-hover">
        <thead>
            <tr style="white-space:nowrap">
                <th style="text-align:center" width="20%">序列KEY</th>
                <th width="20%" style="text-align:center">开始</th>
                <th width="20%" style="text-align:center">结束</th>
                <th width="10%" style="text-align:center">所有APP</th>
                <th width="10%" style="text-align:center">状态</th>
                <th width="20%" style="text-align:center">操作</th>
            </tr>
        </thead>
        <tbody>
        <?php
            if ($series)foreach ($series as $serie)
            {
        ?>
                <tr>
                    <td style="text-align:center"><?php echo $serie['key'];?></td>
                    <td style="text-align:center"><?php echo date('Y-m-d H:i:s', $serie['start']);?></td>
                    <td style="text-align:center"><?php echo date('Y-m-d H:i:s', $serie['end']);?></td>
                    <td style="text-align:center">
                        <i style="width:1em;font-size:9px;color:<?php echo $serie['allApp'] ? '#d43f3a' : '#eea236';?>" class="glyphicon glyphicon-<?php echo $serie['allApp'] ? 'ok' : 'pause';?>"></i>
                    </td>
                    <td style="text-align:center">
                        <i style="width:1em;font-size:9px;color:<?php echo $serie['use'] ? '#d43f3a' : '#eea236';?>" class="glyphicon glyphicon-<?php echo $serie['use'] ? 'ok' : 'pause';?>"></i>
                    </td>
                    <td style="text-align:center">
                        <input class="btn btn-info btn-xs" onclick="javascript:window.location='/admin/series/detail?key=<?php echo $serie['key'];?>';" type="button" value="序列详情"/>
                        <input class="btn btn-info btn-xs" onclick="javascript:window.location='/admin/series/data?key=<?php echo $serie['key'];?>';" type="button" value="统计信息"/>
                    </td>
                </tr>
        <?php
            }
        ?>
        </tbody>
    </table>
</div>

<script type="text/javascript">

</script>
