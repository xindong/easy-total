# Easy Total

这个一个通过监听SQL语句对汇入的日志进行实时处理后重新输出导Fluentd的服务，它是为了解决数据量非常巨大的情况下，数据库无法承载（或存在很大插入压力）巨大的插入请求，并且后期无法做相应的统计功能的问题。

给EasyTotal添加一个SQL语句后，它就可以实时对汇入的数据进行处理并且按照时间间隔条件将运算的结果直接输出，避免了对数据库的巨大插入压力并且解决了业务统计需求。

## 快速使用


### 依赖

需要php环境并安装 swoole 扩展，推荐 php7 + swoole 1.8.4+，需要安装并配置redis或ssdb用于存储运算中的数据，推荐[ssdb](http://ssdb.io/)（因为支持数据落地并且不会像redis那样有内存大小的限制）

### 加入连接

```
ln -s ./easy-total /usr/local/bin/easy-total
ln -s ./server.ini /etc/easy-total.conf
```

### 更改配置

请查看 server.ini 中有详细说明，其中 [server] 为服务器信息设置，运算中的临时数据将会存到 [redis] 配置的服务里，建议不要和其它服务混用

### 启动服务

直接运行 `easy-total` 即可，参数

参数     |  说明
--------|---------------
-c path | 配置文件路径，默认 /etc/easy-total.conf
-l path | 日志路径
-d      | 守护进程化方式启动
--debug | 开启debug


### 使用

#### 监听一个SQL处理语句

```
curl -d 'sql=select *,count(id) as count,dist(id) from test group by type' http://127.0.0.1:9200/task/add
```
将会返回类似json，其中 queryKey 表示当前注册的新的sql的key

```json
{
    "status" : "ok",
    "queryKey" : "703961d9b581471b"
}
```

#### 移除一个SQL处理语句

```
curl -d 'sql=select *,count(id) as count,dist(id) from test group by type' http://127.0.0.1:9200/task/remove
```

## SQL支持的语法
关键字:

* select 
* from
* for
* where
* group time
* group by
* save as
* join on 暂不支持但在计划开发功能之列 

例：
`select field1,field2 as test from where type=1 and (statu = 2 or statu = 3) test group time 3m group by type save to newtable` 

### select 
**select** 部分支持 `*|count|sum|max|min|avg|first|last|dist|exclude|listcount|list` 等方法，除*外其它都支持as

说明

* count - 不管是 count(field1) 还是 count(field2) 都是一样的值，差别是 count(field1) 相当于 count(field1) as field1, count(field1) 相当于 count(field2) as field2
* dist - dist(id) 会直接得到id序列的唯一count数
* exclude - 排除某个字段，比如 select *,exclued(f1),exclude(f2) 将会排除f1和f2两个字段，其它都保留
* list, listcount - 将数据全部放在列表里、对应count数，暂未实现


### from

**from** 目前只支持单表

### for

**for** 是用来指定项目匹配的，不设置则默认全部。比如 `select *,count(count) from test for app1,app2` 时，当log的tag是 `app1.test` 则会解析，而如果是 `app3.test` 则会忽略。

这边需要强调的是， `app1.test` 和 `app2.test` 都符合条件，但是他们的运算数据都是独立的，不会相互干扰的，默认情况下， `app1.test` 将会输出到 `merged.app1.test`，`app2.test` 将会输出到 `merged.app2.test`。

### where

和mysql语句相同，支持复合条件，比如 `where a=1 and (b=2 or b=3) and (d=1 or (e=2 and e = 3))`

支持个别方法，比如

* `a % 10 = 1`
* `a >> 10 = 1`
* `a << 10 = 1`
* `a / b = 1`
* `a - b = 1`
* `a * b = 1`
* `a + b = 1`
* `from_unixtime(a, '%Y-%m-%D %H:%i:%s') = 2016-10-10 10:10:01`
* `unix_timestamp(a) >= 1234567890`

匹配符支持

* `>`
* `<`
* `!=` 或 `<>`
* `>=`
* `<=`

### group by 

根据字段分支，例如 `group by a,b` 表示按a和b两个字段同时进行分组

### group time

这个是一个 *特有* 的关键词，不设置则默认是 group time 1m（1分钟）。它的用途是按时间戳将数据分组，在1组里数据将会和另外一组的数据隔离，也就意味着你去算dist，sum等时，是分开来的。

举个例子：
假设sql语句是 `select *, count(*), dist(title) as dist_title, first(id) as first_id from test`，下面有几条数据

id   |  title  | time （time应该是时间戳，但为便于识别所以写成了以下日期的格式）
-----|---------|-------------------------------
1    | a       | 2016-05-01 20:20:10
2    | a       | 2016-05-01 20:20:20
3    | a       | 2016-05-01 20:21:10
4    | a       | 2016-05-01 20:21:20
5    | b       | 2016-05-01 20:21:30

那么如果 group time 1m (默认) 的话则会产生2条数据分别是

_id          | id   |  title  | time                 | count | dist_title | first_id
-------------|------|---------|----------------------|-------|------------|------------
1m_1234567   | 2    | b       | 2016-05-01 20:20:20  | 2     | 1          | 1
1m_1234568   | 5    | e       | 2016-05-01 20:21:30  | 3     | 2          | 3

那么如果 group time 1d (1天) 即 `select *, count(*), dist(title) as dist_title, first(id) as first_id from test group time 1d save as test_per_1d` ，则会产生1条数据：

_id          | id   |  title  | time                 | count | dist_title | first_id
-------------|------|---------|----------------------|-------|------------|------------
1d_1234567   | 5    | e       | 2016-05-01 20:21:30  | 5     | 2          | 1

（ps: 以上 _id 是系统生成的，1d_1234567 只是一个列子，并不表示一定是它）
鉴于实际的统计需要，可以对同一个数据重复添加相应的sql监听需求以满足业务需求。


### save as

将统计出的数据保存到一个新的关键字序列，默认不写则和来源一样。例如: `select count(*) from test save as newtable` 。

注意，在配置里的 [output] 中可以设置 tag_prefix 输出前缀，例如 `tag_prefix = merged.`，如果请求的tag是
`log.app1.test` 则会输出到 `merged.app1.test`



