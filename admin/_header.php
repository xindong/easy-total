<?php
/**
 * @var Swoole\Http\Request $request
 */
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>EasyTotal - Admin</title>
    <link href="/assets/bootstrap/dist/css/bootstrap.min.css" rel="stylesheet">
    <script type="text/javascript" src="/assets/jquery/dist/jquery.min.js"></script>
    <style type="text/css">
        table.table-bordered td {
            vertical-align:middle !important;
        }
    </style>
</head>
<body>
<nav class="navbar navbar-default">
    <div class="container-fluid">
        <div class="navbar-header">
            <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#bs-example-navbar-collapse-1" aria-expanded="false">
                <span class="sr-only">Toggle navigation</span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
            </button>
            <a class="navbar-brand" href="/admin/">Easy Total</a>
        </div>

        <div class="collapse navbar-collapse" id="bs-example-navbar-collapse-1">
            <ul class="nav navbar-nav">
                <li<?php if ($uri === 'index')echo ' class="active"';?>><a href="/admin/">概览</a></li>
                <li<?php if ($uri === 'task/list' || $uri === 'task/info')echo ' class="active"';?>><a href="/admin/task/list/">任务管理</a></li>
                <li<?php if ($uri === 'series')echo ' class="active"';?>><a href="/admin/series/list/">统计序列管理</a></li>
                <li<?php if ($uri === 'dist')echo ' class="active"';?>><a href="/admin/series/dist">唯一序列管理</a></li>
                <li<?php if ($uri === 'join')echo ' class="active"';?>><a href="/admin/join/">JOIN数据管理</a></li>
                <li<?php if ($uri === 'app')echo ' class="active"';?>><a href="/admin/app/">APP统计</a></li>
                <!--
                <li class="dropdown<?php if ($uri === 'task/list')echo ' active';?>">
                    <a href="/admin/task/list/" class="dropdown-toggle" data-toggle="dropdown" role="button">任务管理 <span class="caret"></span></a>
                    <ul class="dropdown-menu">
                        <li><a href="/admin/task/list/">全部任务</a></li>
                        <li><a href="/admin/task/add/">添加新任务</a></li>
                    </ul>
                </li>
                -->
            </ul>
            <form class="navbar-form navbar-left" action="/admin/task/list/" method="get" role="search">
                <div class="form-group">
                    <input type="text" name="keyword" value="<?php if ($uri === 'task/list')echo htmlentities($request->get['keyword']);?>" class="form-control" placeholder="关键字">
                </div>
                <button type="submit" class="btn btn-primary">搜索任务</button>
            </form>
            <ul class="nav navbar-nav navbar-right">
                <li<?php if ($uri === 'task/add')echo ' class="active"';?>><a href="/admin/task/add/">添加任务</a></li>
                <li><a href="#" style="padding-right:0;">退出管理</a></li>
            </ul>
        </div>
    </div>
</nav>
