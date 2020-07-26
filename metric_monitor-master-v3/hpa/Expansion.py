# -*- coding: utf-8 -*-
import time

import Config
from Logger import Logger

logger = Logger(logname=Config.LOGGER_LOC, loglevel=1, logger="Expansion").getlog()


class Expansion(object):

    # 第一步检查inceptor是否满足扩容规则
    # 第二步检查inceptor executor数量小于最大值，判断为true，则扩容
    @staticmethod
    def is_need_expansion(memoryClient, cpuClient, inceptorClient, namespace, inceptorInstanceName, executorPodNamePrefix, slaLevel, sparkUiUrl):
        # 权重100不需要扩容
        if Config.SLA[slaLevel][0] == 100:
            logger.info("Namespace: %s Inceptor: %s has %s SLA level, Does not need to expansion" % (
        namespace, inceptorInstanceName, str(slaLevel)))
            return False
        logger.info("Begin Check %s/%s If Inceptor Executor Need Expansion\n" % (
            namespace, inceptorInstanceName))
        executor_pod_mem_limit = memoryClient.get_namespace_specify_pod_memory_limit(namespace, executorPodNamePrefix)

        if Expansion.predict_expansion(memoryClient, executorPodNamePrefix, inceptorClient, namespace, inceptorInstanceName, slaLevel, sparkUiUrl, executor_pod_mem_limit):
            logger.info("Begin Verify Rules To Expansion %s/%s Inceptor Executor\n" % (
                namespace, inceptorInstanceName))
            return True
        else:
            logger.info("%s/%s Inceptor Executor Does not Need To Expansion\n" % (
                namespace, inceptorInstanceName))
            return False

    # 1、inceptor running job大于设置值
    # 2、Active Stage大于设定值inceptorClient.get_inceptor_active_stages
    # 3、Pending Stage大于设定值
    # 4、过去一段时间提交的job大于设定值（暂时不需要）
    # 5、过去一段时间提交的stage大于设定值（暂时不需要）
    @staticmethod
    def predict_expansion(memoryClient, executorPodNamePrefix, inceptorClient, namespace, inceptorInstanceName, slaLevel, sparkUiUrl, executor_pod_mem_limit):
        # 如果当前executor数量不小于设置的最大副本数，则跳过检查，不需要扩容
        if len(executor_pod_mem_limit) >= int(Config.EXECUTOR_MAX_REPLICA):
            logger.info("%s/%s Executor Num Must Less Than Default Max Replicas(%s)" % (
                namespace, inceptorInstanceName, str(Config.EXECUTOR_MAX_REPLICA)))
            return False
        end_time = time.time()
        start_time = end_time - float(str(Config.LAST_DURATION_TIME).strip(" ").strip("s"))
        executor_pod_dict = memoryClient.get_namespace_specify_pod_memory_used_range_time(namespace, executorPodNamePrefix, str(start_time), str(end_time))
        totalUse = 0.0
        totalLimit = 0.0
        executor_pod_ip_list = []
        for (key, value) in executor_pod_mem_limit.items():
            tmp_pod_mem_used = str(executor_pod_dict[key]['max_value'])
            logger.info("PodName: %s , Used: %s , Limit %s" % (
                str(key), tmp_pod_mem_used, str(value)))
            totalUse += float(tmp_pod_mem_used)
            totalLimit += float(str(value))
            executor_pod_ip_list.append(str(executor_pod_dict[key]['host_ip']))
        # 计算节点CPU平均使用率，大于50%，则默认不可调度Executor，可调度的节点数要不小于扩容的executor数量
        ip_mem_total_dict = memoryClient.get_all_node_memory_total()
        ip_mem_used_dict = memoryClient.get_all_node_memory_used_range_time(str(start_time), str(end_time))
        total_num = 0
        for (key, value) in ip_mem_used_dict.items():
            if executor_pod_ip_list.__contains__(str(key)):
                continue
            logger.info(
                "NodeIP: %s , Used: %s , Total %s" % (str(key), str(value['max_value']), str(ip_mem_total_dict[key])))
            if float(str(value['max_value'])) / float(str(ip_mem_total_dict[key])) <= float(0.5):
                total_num = total_num + 1
        scale_num = int(int(str(Config.EXECUTOR_MAX_REPLICA)) - int(len(executor_pod_mem_limit)))
        if total_num < int(scale_num):
            logger.info("Available Node Num Smaller Than Expansion Num, Most Node Memory Usage Grater Than 50%, Expansion Cancelled! ")
            return False
        logger.info("Available Node Num: %s, Scale Num: %s", str(total_num), str(scale_num))

        maxRunningJob = str(Config.SLA[slaLevel][4])
        logger.info("No1. Check %s/%s Inceptor Running Job Is Greater Than Default Max Running Job Values: %s" % (
            namespace, inceptorInstanceName, maxRunningJob))
        running_job_dict = inceptorClient.get_inceptor_running_jobs(sparkUiUrl)
        if len(running_job_dict) > int(maxRunningJob):
            logger.info("No1. Check Success. Running Job Values(%s) Greater Than Default Max Running Job Values(%s)" % (
                str(len(running_job_dict)), maxRunningJob))
            return True
        logger.info("No1. Check Fail. Running Job Values(%s) Don't Grater Than Default Max Running Job Values(%s) \n" % (
            str(len(running_job_dict)), maxRunningJob))

        maxActiveStage = str(Config.SLA[slaLevel][5])
        logger.info("No2. Check %s/%s Inceptor Active Stages Is Greater Than Default Max Active Stages Values: %s" % (
            namespace, inceptorInstanceName, maxActiveStage))
        active_stages_dict = inceptorClient.get_inceptor_active_stages(sparkUiUrl)
        if len(active_stages_dict) > int(maxActiveStage):
            logger.info("No2. Check Success. Active Stages Values(%s) Greater Than Default Max Stages Values(%s)" % (
                str(len(active_stages_dict)), maxActiveStage))
            return True
        logger.info("No2. Check Fail. Active Stages Values(%s) Don't Grater Than Default Max Stages Values(%s) \n" % (
            str(len(active_stages_dict)), maxActiveStage))

        maxPendingStage = str(Config.SLA[slaLevel][6])
        logger.info(
            "No3. Check  %s/%s Inceptor Pending Stages Is Greater Than Default Max Pending Stages Values: %s" % (
                namespace, inceptorInstanceName, maxPendingStage))
        pending_stages_dict = inceptorClient.get_inceptor_pending_stages(sparkUiUrl)
        if len(pending_stages_dict) > int(maxPendingStage):
            logger.info(
                "No3. Check Success. Pending Stages Values(%s) Greater Than Default Max Pending Stages Values(%s)" % (
                    str(len(pending_stages_dict)), maxPendingStage))
            return True
        logger.info(
            "No3. Check Fail. Pending Stages Values(%s) Don't Grater Than Default Max Pending Stages Values(%s) \n" % (
                str(len(pending_stages_dict)), maxPendingStage))

        # 如果开启检查过去一段时间job数、stage数检查
        if int(Config.EXPANSION_CHECK_DURATION) == 1:

            logger.info(
                "No4. Check %s/%s Inceptor Submit Jobs Greater Than Default Max Submit Jobs Values: %s ,At Last Duration: %s" % (
                    namespace, inceptorInstanceName, str(Config.EXECUTOR_DURATION_MAX_SUBMIT_JOB),
                    str(Config.LAST_DURATION_TIME)))
            submit_jobs_dict = inceptorClient.get_inceptor_jobs_range_time(sparkUiUrl, start_time)
            if len(submit_jobs_dict) > int(str(Config.EXECUTOR_DURATION_MAX_SUBMIT_JOB)):
                logger.info(
                    "No4. Check Success.  Submit Jobs Values(%s) Greater Than Default Max Submit Jobs Values(%s)" % (
                        str(len(submit_jobs_dict)), str(Config.EXECUTOR_DURATION_MAX_SUBMIT_JOB)))
                return True
            logger.info("No4. Check Fail.  Submit Jobs Values(%s) Don't Grater Than Default Max Submit Jobs Values(%s) \n" % (
                str(len(submit_jobs_dict)), str(Config.EXECUTOR_DURATION_MAX_SUBMIT_JOB)))

            logger.info(
                "No5. Check %s/%s Inceptor Submit Stages Greater Than Default Max Submit Stages Values: %s ,At Last Duration: %s" % (
                    namespace, inceptorInstanceName,
                    str(Config.EXECUTOR_DURATION_MAX_SUBMIT_STAGE), str(Config.LAST_DURATION_TIME)))
            submit_stages_dict = inceptorClient.get_inceptor_jobs_range_time(sparkUiUrl, start_time)
            if len(submit_stages_dict) > int(str(Config.EXECUTOR_DURATION_MAX_SUBMIT_STAGE)):
                logger.info(
                    "No5. Check Success.  Submit Stages Values(%s) Greater Than Default Max Submit Stages Values(%s)" % (
                        str(len(submit_jobs_dict)), str(Config.EXECUTOR_DURATION_MAX_SUBMIT_STAGE)))
                return True
            logger.info(
                "No5. Check Fail.  Submit Stages Values(%s) Don't Grater Than Default Max Submit Stages Values(%s) \n" % (
                    str(len(submit_jobs_dict)), str(Config.EXECUTOR_DURATION_MAX_SUBMIT_STAGE)))

        executorUsage = Config.SLA[slaLevel][7]
        logger.info("No6. Check If %s/%s Inceptor Executor Memory Usage Greater Than %s" % (
            namespace, inceptorInstanceName, str(executorUsage)))
        if totalUse / totalLimit > float(executorUsage/100.0):
            logger.info("No6. Check Success. Executor average Usage Grater Than %s" % str(executorUsage))
            return True
        logger.info("No6. Check Fail. Executor average Usage Don't Grater Than %s" % str(executorUsage))
        return False

    @staticmethod
    def verify_expansion_rules(memoryClient, cpuClient, inceptorClient, namespace, inceptorInstanceName, executor_pod_mem_limit):

        is_expansion = True

        # executor_pod_mem_limit = memoryClient.get_namespace_specify_pod_memory_limit(namespace, executorPodNamePrefix)

        logger.info(
            "Rules 01: %s/%s Executor Num Must Less Than Default Max Replicas(%s)" % (namespace, inceptorInstanceName, str(Config.EXECUTOR_MAX_REPLICA)))
        if len(executor_pod_mem_limit) >= int(Config.EXECUTOR_MAX_REPLICA):
            logger.info("=============================")
            logger.info("Rules 01 Verify Fail")
            logger.info("Pod Replicas Is Already Max, Replicas: " + str(len(executor_pod_mem_limit)))
            logger.info("=============================\n")
            return False
        logger.info("Rules 01: Verify Success \n")

        # end_time = time.time()
        # start_time = end_time - float(str(Config.LAST_DURATION_TIME).strip(" ").strip("s"))

        # logger.info("Rules 02: %s/%s All Executor Pod Memory Used Must Greater Than 0.6 At Last Duration: %s" % (
        # namespace, inceptorInstanceName, str(Config.LAST_DURATION_TIME)))
        # executor_pod_dict = memoryClient.get_namespace_specify_pod_memory_used_range_time(namespace, executorPodNamePrefix, str(start_time), str(end_time))
        #
        # if len(executor_pod_dict) < len(executor_pod_mem_limit):
        #     logger.info("=============================")
        #     logger.info("Rules 02: Verify Fail")
        #     logger.info("Pod Number Is Inconsistent, Pod May Be Expansion Just Now")
        #     logger.info("=============================\n")
        #     return False
        #
        # executor_pod_ip_list = []
        # executorUsage = Config.SLA[slaLevel][7]
        # for (key, value) in executor_pod_mem_limit.items():
        #     tmp_pod_mem_used = str(executor_pod_dict[key]['max_value'])
        #     logger.info("PodName: %s , Used: %s , Limit %s" % (
        #         str(key), tmp_pod_mem_used, str(value)))
        #     if float(tmp_pod_mem_used) / float(str(value)) <= float(executorUsage/100.0):
        #         logger.info("Executor Cannot Be Expansion, Because Pod Max Used Smaller Than " + str(executorUsage) + "% At Last " + str(
        #             Config.LAST_DURATION_TIME) + " Time")
        #         logger.info("=============================")
        #         logger.info("Rules 02 Verify Fail")
        #         logger.info(
        #             "PodName: " + str(key) + " Used Less Than " + str(executorUsage) + "% At Last " + str(Config.LAST_DURATION_TIME) + " Time")
        #         logger.info("=============================\n")
        #         return False
        #     executor_pod_ip_list.append(str(executor_pod_dict[key]['host_ip']))
        # logger.info("Rules 02 Verify Success \n")
        # executor_pod_ip_list = []
        # for (key, value) in executor_pod_mem_limit.items():
        #     executor_pod_ip_list.append(str(executor_pod_mem_limit[key]['host_ip']))
        # logger.info(
        #     "Rules 03: Physical Node Which Can Be Scheduled, Node Memory Used Must Less Than 50% At Last Duration: " + str(
        #         Config.LAST_DURATION_TIME))
        # ip_mem_used_dict = memoryClient.get_all_node_memory_used_range_time(str(start_time), str(end_time))
        # if len(executor_pod_mem_limit) >= len(ip_mem_used_dict):
        #     logger.info("=============================")
        #     logger.info("Rules 03 Verify Fail")
        #     logger.info("Executor Pod Num %s Greater Than Physical Node Num %s" % (
        #         str(len(executor_pod_ip_list)), str(len(ip_mem_used_dict))))
        #     logger.info("=============================\n")
        #     return False
        #
        # ip_mem_total_dict = memoryClient.get_all_node_memory_total()
        # total_num = 0
        # for (key, value) in ip_mem_used_dict.items():
        #     if executor_pod_ip_list.__contains__(str(key)):
        #         continue
        #     logger.info(
        #         "NodeIP: %s , Used: %s , Total %s" % (str(key), str(value['max_value']), str(ip_mem_total_dict[key])))
        #     if float(str(value['max_value'])) / float(str(ip_mem_total_dict[key])) <= float(0.5):
        #         total_num = total_num + 1
        # 一个节点可以启动多个executor，去除检查规则
        # scale_num = int(int(str(Config.EXECUTOR_MAX_REPLICA)) - int(len(executor_pod_mem_limit)))
        # if total_num < int(scale_num):
        #     logger.info("=============================")
        #     logger.info("Rules 03 Verify Fail")
        #     logger.info("May Scheduled Node Number %s Less Than Expansion Capacity %s At Last %s Time" % (
        #         str(total_num), str(scale_num), str(Config.LAST_DURATION_TIME)))
        #     logger.info("=============================\n")
        #     return False
        # logger.info("Rules 03 Verify Success \n")

        # Skip Check Cpu

        return is_expansion

    @staticmethod
    def executor_expansion_final_num(memoryClient, inceptorClient, namespace, inceptorInstanceName):

        executor_pod_mem_limit = memoryClient.get_namespace_specify_pod_memory_limit(namespace, str(
            Config.EXECUTOR_POD_NAME_PREFIX))

        executor_num = len(executor_pod_mem_limit)

        return executor_num, min(int(str(Config.EXECUTOR_MAX_REPLICA)),
                                 int(executor_num + int(str(Config.EXECUTOR_EVERY_INCREASE_OR_DECREASE_CAPACITY))))
