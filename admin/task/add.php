<div class="container">
    <form method="post" class="form-horizontal task-add-form">
        <div class="form-group">
            <div class="col-sm-offset-2 col-sm-10">
                <h4>添加任务</h4>
            </div>
        </div>
        <div class="form-group">
            <label class="col-sm-2 control-label">任务名称</label>
            <div class="col-sm-10">
                <input type="text" name="name" class="form-control" placeholder="名称">
            </div>
        </div>
        <div class="form-group">
            <label class="col-sm-2 control-label">SQL</label>
            <div class="col-sm-10">
                <textarea name="sql" rows="4" class="form-control" placeholder="SQL"></textarea>
            </div>
        </div>
        <div class="form-group">
            <div class="col-sm-offset-2 col-sm-10">
                <div class="checkbox">
                    <label>
                        <input name="merge" value="yes" type="checkbox"> 如果已经有相同规则则合并
                    </label>
                </div>
            </div>
        </div>
        <div class="form-group">
            <div class="col-sm-offset-2 col-sm-10">
                <button type="submit" class="btn btn-primary">提交任务</button>
                &nbsp;
                &nbsp;
                &nbsp;
                &nbsp;
                <a href="/admin/task/list/"><button type="button" class="btn btn-info btn-sm">返回任务列表</button></a>
            </div>
        </div>
        <div class="form-group">
            <div class="col-sm-offset-2 col-sm-10">
                <br />
                <h5>SQL说明:</h5>

                语法关键字（按顺序）:
                <ul>
                    <li>select</li>
                    <li>from</li>
                    <li>for</li>
                    <li>join on 暂不支持</li>
                    <li>where</li>
                    <li>group by</li>
                    <li>group time</li>
                    <li>save as</li>
                </ul>

                例:
                <pre>select field1,field2 as test from test where type=1 and (statu = 2 or statu = 3) and tid in (1,3,5,7,9)
group by type group time 3m save as newtable</pre>

                其中，select 和 from 为必须出现的关键字，其它为可选关键字，`group time` 不设置则默认为 1m，`save as` 不设置则默认和 `from` 相同
        </div>
        </div>
    </form>
</div>

<script type="text/javascript">
    $('.task-add-form').on('submit', function(e)
    {
        e.stopPropagation();
        e.preventDefault();
        var formData = {
            name : this.elements.name.value,
            sql : this.elements.sql.value,
            merge : this.elements.merge.checked ? 'yes' : 'no'
        };

        if (formData.name == '')
        {
            alert('请输入任务名称');
            return false;
        }
        if (formData.sql == '')
        {
            alert('请输入任务SQL');
            return false;
        }
        $.ajax({
            url: '/api/task/add',
            data: formData,
            type: 'post',
            dataType: 'json',
            success: function(data, status, xhr)
            {
                if (data.status == 'error')
                {
                    alert(data.message || '添加失败');
                    return;
                }
                alert('添加成功');
                window.location.reload();
            },
            error: function(xhr, status, err)
            {
                alert('请求服务器失败');
            }
        });

        return false;
    });
</script>