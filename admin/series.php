<?php $queries = array_map('unserialize', $this->worker->redis->hGetAll('queries') ?: []);?>
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
                <th style="text-align:center" width="50">统计类型</th>
                <th width="120" style="text-align:center">时间单位</th>
                <th width="45" style="text-align:center">所属游戏</th>
                <th width="130" style="text-align:center">时间</th>
                <th width="140" style="text-align:center">结果</th>
            </tr>
        </thead>
        <tbody id="ssdb_data_boday"></tbody>
    </table>
    <ul class="pager">
        <li class="disabled"><a href="#">上一页</a></li>
        <li><a href="#">下一页</a></li>
    </ul>
</div>

<script type="text/javascript">
    get_list('');

    function get_list(formData)
    {
        $.ajax({
            url: '/api/task/series',
            data: formData,
            type: 'get',
            dataType: 'json',
            success: function(data, status, xhr)
            {
                if (data.status == 'error')
                {
                    alert(data.message || '查询失败');
                    return false;
                }

                $('#ssdb_data_boday').html('');
                var html = '';
                $.each(data.list, function(name, value) {
                    html += '<tr>';
                    html += '<td style="text-align:center" >'+value.tab+'</td>';
                    html += '<td style="text-align:center" >'+value.time_unit+'</td>';
                    html += '<td style="text-align:center" >'+value.game+'</td>';
                    html += '<td style="text-align:center" >'+value.time+'</td>';
                    html += '<td style="text-align:center" >'+value.result+'</td>'
                    html += '</tr>';
                });
                $('#ssdb_data_boday').html(html);

                $('.pager').html('');
                var pager_html = '';
                pager_html += '<li><a href="javascript:void(0);" onclick="next_page('+formData+');">上一页</a></li>';
                pager_html += '<li><a href="javascript:void(0);" onclick="previous_page('+formData+');">下一页</a></li>';
                $('.pager').html(pager_html);
            },
            error: function(xhr, status, err)
            {
                alert('请求服务器失败');
                return false;
            }
        });
    }

    function next_page(formData)
    {

    }

    function previous_page(formData)
    {

    }
</script>
