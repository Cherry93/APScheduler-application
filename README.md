# APScheduler-application
关于APScheduler应用的一些记录
项目背景：需要做定时任务，实时监控当前任务状态，又没有被修改，被删除，或者有新增任务。
首先从数据库(mysql)取出任务配置，运行的时间以及需要修改的参数
***

``` python
for one in config:
     scheduler.add_job()
```

通过这种方式添加job，如果间隔时间比较近同时有多个job运行，会有个问题
#### 关键点 jitter=30
借鉴了下面blog
https://blog.csdn.net/Panda_813/article/details/82835494
***
还有当数据库被修改了 为什么pymysql重连后才能查到被其他地方修改的数据 pymysql缓存？
https://www.codeleading.com/article/7771828064/
***
如果要实现修改job为20秒写入文件 之前一直都是用：
result = scheduler.reschedule_job(job_id='insert_time',trigger='interval',seconds=20)
这种方法实现的。modify_job无论怎么弄都是不成功！
以下解决方法
https://blog.csdn.net/feixiaohuijava/article/details/78835034
