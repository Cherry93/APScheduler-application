import time
import logging
import datetime
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler

mysql_conn2 = pymysql.connect(host=Host, user=User, password=Passwd, database=BD, charset=CharSet,
                              autocommit=True)
logger = logging.getLogger(__name__)

now_time = time.strftime("%H:%M:%S", time.localtime())
logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)


def func():
    now = datetime.datetime.now()
    ts = now.strftime('%Y-%m-%d %H:%M:%S')
    print('do func  time :', ts)


def func2(test):
    # 耗时2S
    now = datetime.datetime.now()
    ts = now.strftime('%Y-%m-%d %H:%M:%S')
    print('do func2 time：', ts)
    print(test, '-----------')
    time.sleep(2)


def date_to_week():
    # 今天星期几
    from datetime import datetime
    t_str = '2017-03-10 20:24:40'
    d = datetime.strptime(t_str, '%Y-%m-%d %H:%M:%S')
    week = d.weekday()
    return week


def get_config_from_db():
    cursor = mysql_conn2.cursor()
    sql = 'select DATE_FORMAT(a.time1,"%H:%i:%s"),DATE_FORMAT(a.time2,"%H:%i:%s"),DATE_FORMAT(a.startDate,"%Y-%c-%d")' \
          ',DATE_FORMAT(a.endDate,"%Y-%c-%d"),a.timeDimension,a.state1,a.state2,a.email,a.user,b.targetId,' \
          'a.taskId,a.country from Apr_Ad_TimeTasks as a ,Apr_Ad_TimeTarget as b where a.taskId=b.taskId'

    sql2 = 'select DATE_FORMAT(a.time1,"%H:%i:%s"),DATE_FORMAT(a.time2,"%H:%i:%s"),DATE_FORMAT(a.startDate,"%Y-%c-%d")' \
           ',DATE_FORMAT(a.endDate,"%Y-%c-%d"),a.timeDimension,a.state1,a.state2,a.email,a.user,b.targetId,' \
           'a.taskId,a.country from Apr_Ad_TimeTasks as a ,Apr_Ad_TimeTarge_adGroup as b where a.taskId=b.taskId'
    res_list = []
    try:
        ret = cursor.execute(sql)
        logger.info('sql ret@@@@@@@@: %s' % ret)
        ret_data = cursor.fetchall()
        print(ret_data, '&&&&&&&&&&&&&&&&&&&&&&&&&&&')
        for one in ret_data:
            res_list.append({"time1": one[0], "time2": one[1],
                             "startDate": one[2], "endDate": one[3],
                             "timeDimension": one[4], "state1": one[5], "state2": one[6], "email": one[7],
                             "user": one[8], "targetId": one[9], "taskId": one[10], "targetType": 0,
                             'country': one[11]})
        cursor.execute(sql2)
        ret_data2 = cursor.fetchall()
        for one in ret_data2:
            res_list.append({"time1": one[0], "time2": one[1], "startDate": one[2], "endDate": one[3],
                             "timeDimension": one[4], "state1": one[5], "state2": one[6], "email": one[7],
                             "user": one[8], "targetId": one[9], "taskId": one[10], "targetType": 1,
                             'country': one[11]})
    except Exception as e:
        logger.info('get_sku_according_to_code_mysql error: %s' % e)
    finally:
        cursor.close()
        return res_list


def get_name_from_campaign(targetType, targetId):
    cursor = mysql_conn2.cursor()
    print(targetId, targetType, '[][][][][][][][\\\]\[][]====')
    if targetType == 0:
        sql = "select name,dailyBudget from Apr_Campaigns where campaignId='%s'" % targetId
    else:
        sql = "select name,defaultBid from Apr_Adgroups where adgroupId='%s'" % targetId
    print(sql, '-------------------')
    try:

        ret = cursor.execute(sql)
        logger.info('sql ret@@@@@@@@: %s' % ret)
        ret_data = cursor.fetchone()
        print(ret_data, '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        return ret_data
    except Exception as e:
        logger.info('get_name_from_campaign error: %s' % e)
    finally:
        pass


def insert_time_log_mysql(data):
    """
    :return:
    """
    cursor = mysql_conn2.cursor()
    sql = """INSERT INTO Apr_Ad_RunTasks_Log (taskId,taskName,targetId1,targetId2,runTime,planTime,runState,
    user,exception,afterAdjust,beforeAdjust)
                        VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" \
          % (data['taskId'], data['taskName'], data['targetId1'], data['targetId2'], data['runTime'], data['planTime'],
             data['runState'], data['user'], data['exception'], data['afterAdjust'], data['beforeAdjust'])
    logger.info('sql info: %s' % sql)
    print(sql, '**********')
    try:
        print('!!!!------------------------------------------------------------')
        ret = cursor.execute(sql)
        print('***********%%%%%%%%%%')
        print(ret, '***********^^^^^^^^^^^^^^^^^^$$$$$$$$')
        logger.info('sql ret@@@@@@@@: %s' % ret)
        mysql_conn2.commit()
    except Exception as e:
        print(e, '%%%%%%%%%%%%%')
        mysql_conn2.rollback()
        logger.info('insert_time_log_mysql error: %s' % e)


def scheduler_ads_api(state, t_type, cid, country, t_time, task_id, b, a, c):
    # client_id = account.get('client_id')
    # client_secret = account.get('client_secret')
    # access_token = account.get('access_token')
    # refresh_token = account.get('refresh_token')
    # scope = account.get('scope')['us']  # this place put the country param
    # comp_client = Campaigns(client_id, client_secret, access_token, refresh_token, scope)
    # adg_client = AdGroups(client_id, client_secret, access_token, refresh_token, scope)
    ret = according_to_id_query_data(task_id)
    print(ret, '##$##$#%#%#%#%#%#%#%#%')
    task_name, user = ret
    print(task_name, user, '-=-=-=-=-=')
    ad_name, before_adjust = get_name_from_campaign(t_type, cid)
    print(ad_name, before_adjust, '[][][][][][][][][][][][][][][]')
    print(state, '----------------^^^^^^^^')
    if t_type == 0:
        # comp_client.do_refresh_token()
        # param = {"payload": [{"campaignId": int(cid), "dailyBudget": float(state)}], 'spon': "sp"}
        # res1 = comp_client.update_campaigns(param)  # 广告活动预算
        # print(res1.json(), 'res1res1res1res1res1res1res1res1')
        # code = res1.json()[0]['code']
        res1 = [{'code': 'SUCCESS', 'campaignId': 135205954493413}]
        code = res1[0]['code']
        if code == 'SUCCESS':
            # update_campaigns(state, cid)
            pass
            # TODO 插入日志数据 执行时间，定时任务时间，执行状态，异常原因，调整前，调整后，广告活动名称，广告组名称，定时任务名称，任务ID，用户名
        log_data = {"runTime": now_time, "planTime": t_time, "runState": code, "afterAdjust": state,
                    "beforeAdjust": before_adjust, "taskId": task_id, "taskName": task_name, "targetId1": ad_name,
                    "targetId2": "0", "user": user, "exception": code}
        print(log_data)
        insert_time_log_mysql(log_data)

    elif t_type == 1 and state in ["paused", "archived", "enabled"]:
        param = {"payload": [{"adGroupId": int(cid), "state": state}]}
        # res2 = adg_client.update_adgroups(param)  # 广告组状态
        # print(res2.json(), 'res1res1res1res1res1res1res1res1')
        # code = res2.json()[0]['code']
        res1 = [{'code': 'SUCCESS', 'campaignId': 135205954493413}]
        code = res1[0]['code']
        if code == 'SUCCESS':
            # update_adgroups(state, cid)
            pass
        log_data = {"runTime": now_time, "planTime": t_time, "runState": code, "afterAdjust": state,
                    "beforeAdjust": before_adjust, "taskId": task_id, "taskName": task_name,
                    "targetId1": "0", "targetId2": ad_name, "user": user, "exception": code}
        print(log_data)
        insert_time_log_mysql(log_data)
    else:
        param = {"payload": [{"adGroupId": int(cid), "state": float(state)}]}
        # res3 = adg_client.update_adgroups(param)  # 广告组出价
        # print(res3.json(), 'res1res1res1res1res1res1res1res1')
        # code = res3.json()[0]['code']
        res1 = [{'code': 'SUCCESS', 'campaignId': 135205954493413}]
        code = res1[0]['code']
        if code == 'SUCCESS':
            pass
            # update_adgroups(state, cid)
        log_data = {"runTime": now_time, "planTime": t_time, "runState": code, "afterAdjust": state,
                    "beforeAdjust": before_adjust, "taskId": task_id, "taskName": task_name,
                    "targetId1": "0", "targetId2": ad_name, "user": user, "exception": code}
        print(log_data)
        insert_time_log_mysql(log_data)


def according_to_id_query_data(task_id):
    cursor = mysql_conn2.cursor()
    sql = "select name,user from Apr_Ad_TimeTasks where taskId=%s" % task_id
    print(sql, '-------------------------------')
    try:
        ret = cursor.execute(sql)
        logger.info('sql ret@@@@@@@@: %s' % ret)
        ret_data = cursor.fetchone()
        print(ret_data, '&&&^&^&^&%%$%#$#$@#$@111111')
        return ret_data
    except Exception as e:
        logger.info('according_to_id_query_data error: %s' % e)
    finally:
        pass


def update_adgroups(param, a_id):
    cursor = mysql_conn2.cursor()
    if "paused" or "archived" or "enabled" in param:
        sql = """update Apr_Adgroups set state='%s' where adGroupId='%s' """ % (param, a_id)
    else:
        sql = """update Apr_Adgroups set defaultBid='%s' where adGroupId='%s'""" % (param, a_id)
    logger.info('update_campaigns sql info: %s' % sql)
    print(sql, '**********')
    try:
        print('!!!!------------------------------------------------------------')
        ret = cursor.execute(sql)
        print('***********%%%%%%%%%%')
        print(ret, '***********^^^^^^^^^^^^^^^^^^$$$$$$$$')
        logger.info('sql ret@@@@@@@@: %s' % ret)
        mysql_conn2.commit()
    except Exception as e:
        print(e, '%%%%%%%%%%%%%')
        mysql_conn2.rollback()
        logger.info('update_campaigns error: %s' % e)


def update_campaigns(param, c_id):
    cursor = mysql_conn2.cursor()
    sql = """update Apr_Campaigns set dailyBudget='%s' where campaignId='%s'""" % (param, c_id)
    logger.info('update_adgroups sql info: %s' % sql)
    print(sql, '**********')
    try:
        print('!!!!------------------------------------------------------------')
        ret = cursor.execute(sql)
        print('***********%%%%%%%%%%')
        print(ret, '***********^^^^^^^^^^^^^^^^^^$$$$$$$$')
        logger.info('sql ret@@@@@@@@: %s' % ret)
        mysql_conn2.commit()
    except Exception as e:
        print(e, '%%%%%%%%%%%%%')
        mysql_conn2.rollback()
        logger.info('update_campaigns error: %s' % e)


def monitor_jobs(scheduler):
    """
    此函数监控任务是否有修改和删除还有新增
    :param scheduler: 初始scheduler
    :return: None
    """
    # TODO 先获取job列表 再遍历已有的job，看是否需要增删改
    print(scheduler.get_jobs(), '=============!!!!!!!!!!!')
    job_list = scheduler.get_jobs()
    print(job_list, '******************************')
    for job in job_list:
        print(job.id, '[[[[[[[[[[[[[[[[[[[[')
        if job.id == 'test_monitor_jobs':
            print('~~~~~~~~~~~~~~~~~~~~~~~')
            continue
        print(job.args, '-+++++++')
        job_arg = job.args
        ret = get_attributes_for_tasks(job_arg[2], job_arg[1])
        print(ret, '1111retretretretretretretretretretretret')
        if not ret:
            # 删除该任务 继续
            print('删除该任务 继续...........................', job.id)
            scheduler.remove_job(job.id)  # 删除作业
            continue
        time1, time2, start_date, end_date, time_dimension, state1, state2 = ret
        week = time_dimension if time_dimension != '-1' else "0-6"
        print(job_arg[4], [time1, time2], start_date, job_arg[6], end_date, job_arg[7], week, job_arg[8])
        # 在不需要修改出发时间条件下看是否参数变更
        if 'job1' in job.id and job_arg[0] != state1:
            new_arg = list(job_arg)
            new_arg[0] = state1
            print('此处修改 state1。。。。。。', new_arg[0], state1)
            scheduler.modify_job(job.id, jobstore=None, args=new_arg)  # job1参数变更
        elif 'job2' in job.id and job_arg[0] != state2:
            new_arg = list(job_arg)
            new_arg[0] = state2
            print('此处修改 state2。。。。。。。', job_arg[0], state2)
            scheduler.modify_job(job.id, jobstore=None, args=new_arg)  # job2参数变更
        else:
            print('@@@@@@@不需要变更参数@@@@@@@@')
        if job_arg[4] in [time1, time2] and start_date == job_arg[6] and end_date == job_arg[7] and week == \
                job_arg[8]:
            print('不需要重新执行任务。。。。。。。。。。。')
        else:
            # TODO BUG 由于上个agr 是 agr参数来的 所以这里重新执行任务也要改args 已经处理
            print('重新执行任务。。。。。。。。。。。。。。。。。。。。。。。。。。。')
            # TODO 生产环境需要修改此处
            # scheduler.reschedule_job(job.id, 'interval', seconds=10, jitter=30)  # 修改单个作业的触发器并更新下次运行时间
            # temp_dict = {"seconds": 20}
            new_arg = list(job_arg)
            if 'job1' in job.id:
                hour = time1[:2]
                minute = time1[3:5]
                new_arg[4] = time1
            else:
                hour = time2[:2]
                minute = time2[3:5]
                new_arg[4] = time2
            temp_dict = {"hour": hour, "minute": minute, "start_date": start_date, "end_date": end_date,
                         "day_of_week": week}
            new_arg[6] = start_date
            new_arg[7] = end_date
            new_arg[8] = week
            temp_trigger = scheduler._create_trigger(trigger='cron', trigger_args=temp_dict)
            result = scheduler.modify_job(job_id=job.id, trigger=temp_trigger, args=new_arg)
            print(result, '$$$$$$$$$$$$$$$result')
            time.sleep(10)
    new_job_monitor(scheduler, job_list)


def new_job_monitor(scheduler, job_list):
    """
    这是监控有没有新的任务进来
    :param scheduler: 原始scheduler
    :param job_list: 当前的job list
    :return: None
    """
    res_list = get_config_from_db()
    print(res_list, '@@@@@@@@122222222222222222222222', len(res_list))
    job_id_list = set([job.id.replace('job1-', "").replace('job2-', '') for job in job_list])
    print(job_id_list, '!!!!!!!!!!!!')
    for one_job in res_list:
        print(one_job['targetId'], '*********targetId')
        if one_job['targetId'] not in job_id_list:
            print('新增job。。。。。。。。。。。。。。')
            print('新增job。。。。。。。。。。。。。。')
            add_jobs(scheduler, one_job)
        else:
            print('此次监控没有新增job。。。。。。。。。。。。。。。')


def get_attributes_for_tasks(target_id, t_type):
    """
    判断这个任务是否还在如果不在就删除了一个sql查询
    :param target_id: targetId
    :param t_type: campaign或者ad_group
    :return: ret_data 包含time1，time2，startDate，endDate，timeDimension，state1，state2
    """
    cursor = mysql_conn2.cursor()

    sql = 'select DATE_FORMAT(a.time1,"%H:%i:%s"),DATE_FORMAT(a.time2,"%H:%i:%s"),DATE_FORMAT(a.startDate,"%Y-%c-%d")' \
          ',DATE_FORMAT(a.endDate,"%Y-%c-%d"),a.timeDimension ,a.state1,a.state2 from Apr_Ad_TimeTasks as a ,Apr_Ad_TimeTarget ' \
          f'as b where a.taskId=b.taskId and b.targetId={target_id}'

    sql2 = 'select DATE_FORMAT(a.time1,"%H:%i:%s"),DATE_FORMAT(a.time2,"%H:%i:%s"),DATE_FORMAT(a.startDate,"%Y-%c-%d")' \
           ',DATE_FORMAT(a.endDate,"%Y-%c-%d"),a.timeDimension ,a.state1,a.state2 from Apr_Ad_TimeTasks as a ,Apr_Ad_TimeTarge_adGroup ' \
           f'as b where a.taskId=b.taskId and b.targetId={target_id}'
    if t_type == 0:
        sql_str = sql
    else:
        sql_str = sql2
    print(sql_str, '-------------------------------2')
    try:
        ret = cursor.execute(sql_str)
        logger.info('sql ret@@@@@@@@: %s' % ret)
        ret_data = cursor.fetchone()
        print(ret_data, '&&&^&^&^&%%$%#$#$@#$@')
        return ret_data
    except Exception as e:
        logger.info('according_to_id_query_data error: %s' % e)
    finally:
        pass


def add_jobs(scheduler, one):
    """
    新增job函数
    :param scheduler: 初始创建的scheduler
    :param one: 数据对象
    :return: None
    """
    week = one['timeDimension'] if one['timeDimension'] != '-1' else "0-6"
    print(week, '===================')
    print(one['targetId'], '当前的targetId。。。。。。。。。。。。。')
    if one['time1']:
        hour1 = one['time1'][:2]
        minute1 = one['time1'][3:5]
        scheduler.add_job(scheduler_ads_api, 'cron', hour=hour1, minute=minute1, start_date=one['startDate'],
                          end_date=one['endDate'], day_of_week=week, jitter=30, id=f"job1-{one['targetId']}",
                          args=[one['state1'], one['targetType'],
                                one['targetId'], one['country'],
                                one['time1'], one['taskId'],
                                one['startDate'], one['endDate'], week])
    if one['time2']:
        hour1 = one['time2'][:2]
        minute1 = one['time2'][3:5]
        scheduler.add_job(scheduler_ads_api, 'cron', hour=hour1, minute=minute1, start_date=one['startDate'],
                          end_date=one['endDate'], day_of_week=week, jitter=30, id=f"job2-{one['targetId']}",
                          args=[one['state2'], one['targetType'],
                                one['targetId'], one['country'],
                                one['time2'], one['taskId'],
                                one['startDate'], one['endDate'], week])


def dojob():
    """
    main主入口
    :return: None
    """
    # 创建调度器：BlockingScheduler
    scheduler = BlockingScheduler(executors=executors)
    res_list = get_config_from_db()
    print(res_list, '@@@@@@@@', len(res_list))
    for index, one in enumerate(res_list):
        add_jobs(scheduler, one)
        # testing args 添加 start_date 和 end_date是为了后面判断时间是否被修改了
        # if one['state2']:
        #     scheduler.add_job(scheduler_ads_api, 'interval', seconds=100 + index * 10, jitter=30, replace_existing=True,
        #                       id=f"job2-{one['targetId']}",
        #                       args=[one['state2'], one['targetType'],
        #                             one['targetId'], one['country'],
        #                             one['time2'], one['taskId'],
        #                             one['startDate'], one['endDate'],
        #                             week])
    scheduler.add_job(monitor_jobs, 'interval', replace_existing=True, seconds=60, id='test_monitor_jobs',
                      args=[scheduler])
    scheduler.start()


dojob()
