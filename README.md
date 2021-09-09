# mysql数据库向其他数据库迁移

## 目的

将mysql数据库表结构和数据，直接迁移到其他数据库中

## 前提

mysql数据库中的一些关键字在其他数据库中是兼容的

## 目前

目前只实现了mysql向openGauss数据库的转换

## 使用方式

代码：运行main方法，通过Swing 界面，输入源数据库和目标数据库即可转换

jar，通过源代码maven install即可打出可执行jar

根目录生成log文件，提供日志

## 实现和扩展

通过jdbc方式进行数据库读取和更新，通过SqlTransferService实现各个数据库的语法转换

## 效果图

![](C:\Users\Administrator\Desktop\效果图.jpg)