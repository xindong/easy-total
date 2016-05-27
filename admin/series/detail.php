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
            throw new Exception("无此序列信息详情!");
        }

        $data = array('status' => true);
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
    }
?>

    <div class="panel panel-success">
        <div class="panel-heading">
            <h3 class="panel-title">基本信息</h3>
        </div>
        <div class="panel-body">
            <form class="form-horizontal" id="edit_series">
                <div class="form-group">
                    <label class="col-sm-2 control-label">key:</label>
                    <div class="col-sm-2">
                        <span><?php echo $serieDetail['key'];?></span>
                        <input type="hidden" name="key" class="form-control" placeholder="key" value="<?php echo $serieDetail['key'];?>" />
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-sm-2 control-label">状态:</label>
                    <div class="col-sm-2">
                        <select name="use" class="form-control" >
                            <option value="1" <?php echo $serieDetail['use']==true?'selected':'';?>>是</option>
                            <option value="0" <?php echo $serieDetail['use']==false?'selected':'';?>>否</option>
                        </select>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-sm-2 control-label">开始:</label>
                    <div class="col-sm-2">
                        <input type="text" name="start" class="form-control" placeholder="开始" value="<?php echo $serieDetail['start'];?>" />
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-sm-2 control-label">结束:</label>
                    <div class="col-sm-2">
                        <input type="text" name="end" class="form-control" placeholder="开始" value="<?php echo $serieDetail['end'];?>" />
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-sm-2 control-label">所有APP:</label>
                    <div class="col-sm-2">
                        <select name="allApp" class="form-control" >
                            <option value="1" <?php echo $serieDetail['allApp']==true?'selected':'';?>>是</option>
                            <option value="0" <?php echo $serieDetail['allApp']==false?'selected':'';?>>否</option>
                        </select>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-sm-2 control-label">for:</label>
                    <div class="col-sm-2">
                        <?php
                            echo '<pre style="background: #fcfcfc"><code class="json">';
                            echo htmlspecialchars(json_encode($serieDetail['for'], JSON_PRETTY_PRINT|JSON_UNESCAPED_UNICODE));
                            echo '</code></pre>';
                        ?>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-sm-2 control-label">table:</label>
                    <div class="col-sm-2">
                        <span><?php echo $serieDetail['table'];?></span>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-sm-2 control-label">where:</label>
                    <div class="col-sm-2">
                        <?php
                            echo '<pre style="background: #fcfcfc"><code class="json">';
                            echo htmlspecialchars(json_encode($serieDetail['for'], JSON_PRETTY_PRINT|JSON_UNESCAPED_UNICODE));
                            echo '</code></pre>';
                        ?>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-sm-2 control-label">groupBy:</label>
                    <div class="col-sm-2">
                        <?php
                            echo '<pre style="background: #fcfcfc"><code class="json">';
                            echo htmlspecialchars(json_encode($serieDetail['groupBy'], JSON_PRETTY_PRINT|JSON_UNESCAPED_UNICODE));
                            echo '</code></pre>';
                        ?>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-sm-2 control-label">groupTime:</label>
                    <div class="col-sm-2">
                        <?php
                            echo '<pre style="background: #fcfcfc"><code class="json">';
                            echo htmlspecialchars(json_encode($serieDetail['groupTime'], JSON_PRETTY_PRINT|JSON_UNESCAPED_UNICODE));
                            echo '</code></pre>';
                        ?>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-sm-2 control-label">function:</label>
                    <div class="col-sm-2">
                        <?php
                            echo '<pre style="background: #fcfcfc"><code class="json">';
                            echo htmlspecialchars(json_encode($serieDetail['function'], JSON_PRETTY_PRINT|JSON_UNESCAPED_UNICODE));
                            echo '</code></pre>';
                        ?>
                    </div>
                </div>
                <div class="form-group">
                    <label class="col-sm-2 control-label">queries:</label>
                    <div class="col-sm-2">
                        <?php
                            echo '<pre style="background: #fcfcfc"><code class="json">';
                            echo htmlspecialchars(json_encode($serieDetail['queries'], JSON_PRETTY_PRINT|JSON_UNESCAPED_UNICODE));
                            echo '</code></pre>';
                        ?>
                    </div>
                </div>
                <div class="form-group">
                    <div class="col-sm-offset-2 col-sm-10">
                        <button type="submit" class="btn btn-default">更新</button>
                    </div>
                </div>
            </form>
        </div>
    </div>
</div>

<script>
    $('#edit_series').on('submit', function(e)
    {
        var formData = {
            key    : this.elements.key.value,
            use    : this.elements.use.value,
            allApp : this.elements.allApp.value,
            start  : this.elements.start.value,
            end    : this.elements.end.value
        };

        $.ajax({
            url: '/api/series/edit',
            data: formData,
            type: 'post',
            dataType: 'json',
            success: function(data, status, xhr)
            {
                if (data.status == 'error')
                {
                    alert(data.message || '操作失败');
                    return;
                }
                alert('操作成功');
                window.location.reload();
            },
            error: function(xhr, status, err)
            {
                alert('请求服务器失败');
            }
        });
    });
</script>