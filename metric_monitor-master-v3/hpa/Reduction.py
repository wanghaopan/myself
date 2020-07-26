# -*- coding: utf-8 -*-
import time

import Config
from Logger import Logger

logger = Logger(logname=Config.LOGGER_LOC, loglevel=1, logger="Reduction").getlog()


class Reduction(object):

    # 第一步检查inceptor是否满足缩容规则
    # 第二步检查inceptor executor数量大于等于最大值
    @staticmethod
    def is_need_reduction(memoryClient, cpuClient, inceptorClient, namespace, inceptorInstanceName, executorPodNamePrefix, slaLevel, sparkUiUrl):
        # 如果权重为100，则不需要缩容
        if Config.SLA[slaLevel][0] == 100:
            logger.info("Namespace: %s Inceptor: %s has %s SLA level, does not need to reduction" % (
        namespace, inceptorInstanceName, str(slaLevel)))
            return False
        logger.info("Begin Check %s/%s Inceptor Executor Is Need Reduction\n" % (
        str(namespace), str(inceptorInstanceName)))
        if Reduction.predict_reduction_v2(inceptorClient, namespace, inceptorInstanceName, slaLevel, sparkUiUrl):
            logger.info("Begin Verify Rules To Reduction Inceptor Executor\n")
            return Reduction.verify_reduction_rules(memoryClient, cpuClient, namespace, inceptorInstanceName, executorPodNamePrefix)
        else:
            logger.info("Inceptor Executor No Need To Reduction\n")
            return False

    # 缩容需要满足
    # 1. 当前active job num <= 0
    # 2. 过去一段时间job num <= 0
    @staticmethod
    def predict_reduction_v2(inceptorClient, namespace, inceptorInstanceName, slaLevel, sparkUiUrl):
        logger.info("No1. Check %s/%s Inceptor Running job Num" % (str(namespace), str(inceptorInstanceName)))
        running_job_dict = inceptorClient.get_inceptor_running_jobs(sparkUiUrl)
        if len(running_job_dict) > 0:
            logger.info("=============================")
            logger.info("No1. Check Fail. Found Inceptor Has Running Job")
            logger.info("Running Jobs Number: %s " %(str(len(running_job_dict))))
            logger.info("=============================\n")
            return False
        logger.info("No1. Check Success... Inceptor Don't have Running Job\n")

        end_time = time.time()
        start_time = end_time - float(str(Config.LAST_DURATION_TIME).strip(" ").strip("s"))

        logger.info("No2. Check If %s/%s Inceptor Has Submit Jobs At Last Duration: %s" % (
        namespace, inceptorInstanceName, str(Config.LAST_DURATION_TIME)))
        submit_jobs_dict = inceptorClient.get_inceptor_jobs_range_time(sparkUiUrl, start_time)
        if len(submit_jobs_dict) > 0:
            logger.info("=============================")
            logger.info("No2. Check Fail.  Found Inceptor Submitted Jobs Num Grater than 0")
            logger.info("Submit Jobs Number: %s " %(str(len(submit_jobs_dict))))
            logger.info("=============================\n")
            return False
        logger.info("No2. Check Success... Inceptor Don't have Submitted Jobs\n")

        return True
    @staticmethod
    def predict_reduction(inceptorClient, namespace, inceptorInstanceName, sparkUiUrl):

        logger.info("No1. Check %s/%s Inceptor Is Has Running Job" % (
        namespace, inceptorInstanceName))
        running_job_dict = inceptorClient.get_inceptor_running_jobs(sparkUiUrl)
        if len(running_job_dict) > 0:
            logger.info("=============================")
            logger.info("No1. Check Fail. Found Inceptor Has Running Job")
            logger.info("Running Jobs Number: %s " %(str(len(running_job_dict))))
            logger.info("=============================\n")
            return False
        logger.info("No1. Check Success... Inceptor Has Not Running Job\n")

        # logger.info("No2. Check %s/%s Inceptor Is Has Active Stages" % (
        # str(Config.NAMESPACE), str(Config.INCEPTOR_INSTANCE_NAME)))
        # active_stages_dict = inceptorClient.get_inceptor_active_stages()
        # if len(active_stages_dict) > 0:
        #     logger.info("=============================")
        #     logger.info("No2. Check Fail.  Found Inceptor Has Active Stages")
        #     logger.info("Active Stages Number: %s " %(str(len(active_stages_dict))))
        #     logger.info("=============================\n")
        #     return False
        # logger.info("No2. Check Success... Inceptor Has Not Active Stages\n")

        # logger.info("No3. Check %s/%s Inceptor Is Has Pending Stages" % (
        # str(Config.NAMESPACE), str(Config.INCEPTOR_INSTANCE_NAME)))
        # pending_stages_dict = inceptorClient.get_inceptor_pending_stages()
        # if len(pending_stages_dict) > 0:
        #     logger.info("=============================")
        #     logger.info("No3. Check Fail. Found Inceptor Has Pending Stages")
        #     logger.info("Pending Stages Number: %s " %(str(len(pending_stages_dict))))
        #     logger.info("=============================\n")
        #     return False
        # logger.info("No3. Check Success... Inceptor Has Not Pending Stages\n")

        end_time = time.time()
        start_time = end_time - float(str(Config.LAST_DURATION_TIME).strip(" ").strip("s"))

        logger.info("No4. Check %s/%s Inceptor Is Has Submit Jobs At Last Duration: %s" % (
        namespace, inceptorInstanceName, str(Config.LAST_DURATION_TIME)))
        submit_jobs_dict = inceptorClient.get_inceptor_jobs_range_time(sparkUiUrl, start_time)
        if len(submit_jobs_dict) > 0:
            logger.info("=============================")
            logger.info("No4. Check Fail.  Found Inceptor Has Submit Jobs")
            logger.info("Submit Jobs Number: %s " %(str(len(submit_jobs_dict))))
            logger.info("=============================\n")
            return False
        logger.info("No4. Check Success... Inceptor Has Not Found Any Jobs\n")

        # logger.info("No5. Check  %s/%s Inceptor Is Has Stages At Last Duration: %s " % (
        # str(Config.NAMESPACE), str(Config.INCEPTOR_INSTANCE_NAME), str(Config.LAST_DURATION_TIME)))
        # submit_stages_dict = inceptorClient.get_inceptor_jobs_range_time(start_time)
        # if len(submit_stages_dict) > 0:
        #     logger.info("=============================")
        #     logger.info("No5. Check Fail. Found Inceptor Has Stages")
        #     logger.info("Submit Stages Number: %s " %(str(len(submit_stages_dict))))
        #     logger.info("=============================\n")
        #     return False
        # logger.info("No5. Check Success... Inceptor Has Not Found Any Stages\n")

        return True

    # 当前executor pod数量大于等于最大值，则可以缩容
    @staticmethod
    def verify_reduction_rules(memoryClient, cpuClient, namespace, inceptorInstanceName, executorPodNamePrefix):

        executor_pod_mem_limit = memoryClient.get_namespace_specify_pod_memory_limit(namespace, executorPodNamePrefix)

        logger.info("Rules 01:  %s/%s Executor Num Must Greater Than Default Minimum Replicas(%s)" % (
        namespace, inceptorInstanceName, executorPodNamePrefix))
        if len(executor_pod_mem_limit) >= int(Config.EXECUTOR_MAX_REPLICA):
            logger.info("Rules 01: Verify Success \n")
        else:
            logger.info("=============================")
            logger.info("Rules 01 Verify Fail")
            logger.info("Pod Replicas Is Already Minimum, Replicas: " + str(len(executor_pod_mem_limit)))
            logger.info("=============================\n")
            return False

        # end_time = time.time()
        # start_time = end_time - float(str(Config.LAST_DURATION_TIME).strip(" ").strip("s"))

        # logger.info("Rules 02: %s/%s All Executor Pod Memory Used Must Less Than 0.4 At Last Duration: %s " % (
        # str(Config.NAMESPACE), str(Config.INCEPTOR_INSTANCE_NAME), str(Config.LAST_DURATION_TIME)))
        # executor_pod_dict = memoryClient.get_namespace_specify_pod_memory_used_range_time(str(Config.NAMESPACE), str(
        #     Config.EXECUTOR_POD_NAME_PREFIX), str(start_time), str(end_time))
        #
        # if len(executor_pod_dict) < len(executor_pod_mem_limit):
        #     logger.info("=============================")
        #     logger.info("Rules 02: Verify Fail")
        #     logger.info("Pod Number Is Inconsistent, Pod May Be Expansion Just Now")
        #     logger.info("=============================\n")
        #     return False
        #
        # for (key, value) in executor_pod_mem_limit.items():
        #     tmp_pod_mem_used = str(executor_pod_dict[key]['max_value'])
        #     logger.info("PodName: %s , Used: %s , Limit %s" % (str(key), tmp_pod_mem_used, str(value)))
        #     if float(tmp_pod_mem_used) / float(str(value)) >= float(0.4):
        #         logger.info("=============================")
        #         logger.info("Rules 02 Verify Fail")
        #         logger.info("PodName: " + str(key) + " Used More Than 40% At Last " + str(
        #             Config.LAST_DURATION_TIME) + " Time")
        #         logger.info("=============================\n")
        #         return False
        # logger.info("Rules 02 Verify Success \n")

        # executor_pod_cpu_limit = cpuClient.get_namespace_specify_pod_cpu_limit(Config.NAMESPACE,
        #                                                                        Config.EXECUTOR_POD_NAME_PREFIX)
        # logger.info("Rules 03:  %s/%s All Executor Pod Cpu Used Must Less Than 0.4 At Last Duration: %s " % (
        # str(Config.NAMESPACE), str(Config.INCEPTOR_INSTANCE_NAME), str(Config.LAST_DURATION_TIME)))
        # executor_pod_dict = cpuClient.get_namespace_specify_pod_cpu_used_range_time(str(Config.NAMESPACE), str(
        #     Config.EXECUTOR_POD_NAME_PREFIX), str(start_time), str(end_time))
        #
        # for (key, value) in executor_pod_cpu_limit.items():
        #     tmp_pod_mem_used = str(executor_pod_dict[key]['max_value'])
        #     logger.info("PodName: %s , Used: %s , Limit %s" % (str(key), tmp_pod_mem_used, str(value)))
        #     if float(tmp_pod_mem_used) / float(str(value)) >= float(0.4):
        #         logger.info("=============================")
        #         logger.info("Rules 03 Verify Fail")
        #         logger.info("PodName: " + str(key) + " Used More Than 40% At Last " + str(
        #             Config.LAST_DURATION_TIME) + " Time")
        #         logger.info("=============================\n")
        #         return False
        # logger.info("Rules 03 Verify Success \n")

        return True

    @staticmethod
    def executor_reduction_final_num(memoryClient, inceptorClient, namespace, inceptorInstanceName):

        executor_pod_mem_limit = memoryClient.get_namespace_specify_pod_memory_limit(namespace, str(
            Config.EXECUTOR_POD_NAME_PREFIX))

        executor_num = len(executor_pod_mem_limit)

        return executor_num, max(int(str(Config.EXECUTOR_MIN_REPLICA)),
                                 int(executor_num - int(str(Config.EXECUTOR_EVERY_INCREASE_OR_DECREASE_CAPACITY))))
