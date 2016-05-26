<?php

    //$test = $this->worker->redis->scan($val, "*", 11);

//    $start = '';
//    while(1){
//        $kvs = $this->worker->redis->scan($start, '*', 5);
//        if(!$kvs){
//            break;
//        }
//        // do something on key-value pairs...
//        $keys = array_keys(array_slice($kvs, -1, 1, true));
//        $max_key = $keys[0];
//        $start = $max_key;
//
//        echo "<pre>";
//        print_r($start);
//    }


//    $start = 3;
//    $kvs = $this->worker->redis->scan($start, '*', 4);
//
//    $keys = array_keys(array_slice($kvs, -1, 1, true));
//
//    $max_key = $keys[0];
//
//    echo"<pre>";
//    print_r($kvs);
//    echo"<pre>";
//    print_r($start);
//    echo"<pre>";
//    print_r($keys);
//    echo"<pre>";
//    print_r($max_key);

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
                <th style="text-align:center" width="50">统计类型</th>
                <th width="120" style="text-align:center">时间单位</th>
                <th width="45" style="text-align:center">所属游戏</th>
                <th width="130" style="text-align:center">时间</th>
                <th width="140" style="text-align:center">结果</th>
            </tr>
        </thead>
        <tbody id="ssdb_data_boday"></tbody>
    </table>
    <ul class="pager"></ul>
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

                var html = '';
                $.each(data.list, function(name, value) {
                    html += '<tr>';
                    html += '<td style="text-align:center" >'+value.tab+'</td>';
                    html += '<td style="text-align:center" >'+value.time_unit+'</td>';
                    html += '<td style="text-align:center" >'+value.game+'</td>';
                    html += '<td style="text-align:center" >'+value.time+'</td>';
                    html += '<td style="text-align:center" >'+value.result+'</td>';
                    html += '</tr>';
                });
                $('#ssdb_data_boday').append(html);

                $('.pager').html('');
                var pager_html = '';
                if (data.is_ssdb == false)
                {
                    if (data.next_iterator == 0)
                    {
                        pager_html += '<li class="disabled"><a href="javascript:void(0);">更多</a></li>';
                    }else{
                        pager_html += '<li><a href="javascript:void(0);" onclick="next_page(\'' + data.next_iterator + '\');">更多</a></li>';
                    }

                    pager_html += '<center>prev_iterator:'+data.curr_iterator+',next_iterator:'+data.next_iterator+'</center>';
                }else{

                }

                $('.pager').html(pager_html);
            },
            error: function(xhr, status, err)
            {
                alert('请求服务器失败');
                return false;
            }
        });
    }

    function next_page(next_iterator)
    {
        var formData = {
            page_type     : 'next',
            next_iterator : next_iterator
        };
        get_list(formData);
    }
</script>
