# -*- coding: utf-8 -*-

LOGGER_LOC = "/tmp/metrics_monitor.log"

PROMETHEUS_URL = "http://10.168.25.194:31601"

# 扩容时是否考虑过去一段时间提交的job/stage，默认为false/0，设置为1，则开启检查
EXPANSION_CHECK_DURATION = 0
LAST_DURATION_TIME = "600s"

INCEPTOR_DBASERVICE_URL = ""
INCEPTOR_SPARK_UI_URL = "http://10.168.25.194:31043"
INCEPTOR_SERVER_UUID = ""

NAMESPACE = "demo"
INCEPTOR_INSTANCE_NAME = "clus-245--clus-245-inceptor-5-2-77c3"

EXECUTOR_MIN_REPLICA = 1
EXECUTOR_MAX_REPLICA = 3

EXECUTOR_EVERY_INCREASE_OR_DECREASE_CAPACITY = 2
EXECUTOR_POD_NAME_PREFIX = "executor-6sm7g"

EXECUTOR_MAX_ACTIVE_STAGE = 2
EXECUTOR_MAX_PENDING_STAGE = 2
EXECUTOR_MAX_RUNNING_JOB = 2
EXECUTOR_DURATION_MAX_SUBMIT_JOB = 2
EXECUTOR_DURATION_MAX_SUBMIT_STAGE = 2
# 第二~第四是缩容指标，第五~第八是扩容指标，第九~第十是扩容考虑过去一段时间job、过去一段时间stage，默认不检查，暂时未添加配置。
# [权重,active job num, 过去一段时间job num, pending stage num, active job num, active stage num, pending stage num, executor usage,duration job,duration stage]
# sla级别从0开始，0-4
SLA = [20,4,8,0,10,20,7,80,0,0],[40,3,6,0,8,16,5,60,0,0],[60,2,4,0,6,12,3,50,0,0],[80,1,2,0,4,8,1,40,0,0],[100,0,0,0,0,0,0,0,0,0]

#[namespace,instance,sla level,spark-ui, executor_pod_name_prefix]
INCEPTOR_INFO_LEN = 5
INCEPTOR_LIST = ["demo","clus-245--clus-245-inceptor-5-2-77c3",1,"http://10.168.25.194:31043", "executor-6sm7g"],[]

# /api/inceptor/servers
