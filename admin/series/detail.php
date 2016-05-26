<?php
    try
    {
        $seriesKey = $this->request->get['key'];

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
            throw new Exception("无次序列信息详情!");
        }

        $data = array('status' => true, 'serie_detail' => $serieDetail);
    }
    catch (Exception $e)
    {
        $data = array('status' => false, 'message' => $e->getMessage());
    }
?>

<div style="padding:0 15px;margin-top:-15px">
    <div class="row">
        <div class="col-md-12">
            <div class="pull-left" style="padding:10px 0 0 0">
                <a href="/admin/">管理首页</a> <i style="font-size:11px;width:1em" class="glyphicon glyphicon-menu-right"></i>
                <a href="/admin/series/list/">任务列表</a> <i style="font-size:11px;width:1em" class="glyphicon glyphicon-menu-right"></i>
                统计序列详情
            </div>
        </div>
    </div>
<?php
    if (!$data['status'])
    {
?>
        <p class="bg-danger"><?php echo $data['message'];?></p>
<?php
    }else{
?>
        <div class="col-md-12">
            <h4>序列配置详情</h4>
            <?php
                echo '<pre style="background: #fcfcfc"><code class="json">';
                echo htmlspecialchars(json_encode($serieDetail, JSON_PRETTY_PRINT|JSON_UNESCAPED_UNICODE));
                echo '</code></pre>';
            ?>
        </div>
<?php
    }
?>
</div>