# -*- coding: utf-8 -*-
import commands
import sys
import time
import math

import Config
from Logger import Logger
from client.CpuClient import CpuClient
from client.InceptorClient import InceptorClient
from client.MemoryClient import MemoryClient
from hpa.Expansion import Expansion
from hpa.Reduction import Reduction

logger = Logger(logname=Config.LOGGER_LOC, loglevel=1, logger="Main").getlog()


def init_client():
    memoryClient = MemoryClient()
    cpuClient = CpuClient()
    inceptorClient = InceptorClient()

    return memoryClient, cpuClient, inceptorClient


def predict_inceptor_expansion_or_reduction(memoryClient, cpuClient, inceptorClient, namespace, inceptorInstanceName, slaLevel, sparkUiUrl, executorPodNamePrefix):

    executor_pod_mem_limit = memoryClient.get_namespace_specify_pod_memory_limit(namespace, executorPodNamePrefix)

    logger.info("%s/%s Inceptor Executor Current Number: %s" % (namespace, inceptorInstanceName, str(len(executor_pod_mem_limit))))
    # logger.info("%s/%s Inceptor Executor Can Expansion To Max Number: %s" % (namespace, inceptorInstanceName, str(Config.EXECUTOR_MAX_REPLICA)))
    # logger.info("%s/%s Inceptor Executor Can Reduction To Min Number: %s \n" % (namespace, inceptorInstanceName, str(Config.EXECUTOR_MIN_REPLICA)))

    if len(executor_pod_mem_limit) >= int(str(Config.EXECUTOR_MAX_REPLICA)):

        if Reduction.is_need_reduction(memoryClient, cpuClient, inceptorClient, namespace, inceptorInstanceName, executorPodNamePrefix, slaLevel, sparkUiUrl):
            return -1
    else:
        if Expansion.is_need_expansion(memoryClient, cpuClient, inceptorClient, namespace, inceptorInstanceName, executorPodNamePrefix, slaLevel, sparkUiUrl):
            return 1

    # elif currentPodNum <= int(str(Config.EXECUTOR_MIN_REPLICA)):
    #
    #     if Expansion.is_need_expansion(memoryClient, cpuClient, inceptorClient, namespace, inceptorInstanceName, executorPodNamePrefix, slaLevel, sparkUiUrl):
    #         return 1
    # else:
    #
    #     if Reduction.is_need_reduction(memoryClient, cpuClient, inceptorClient, namespace, inceptorInstanceName, executorPodNamePrefix, slaLevel, sparkUiUrl):
    #         return -1
    #
    #     if Expansion.is_need_expansion(memoryClient, cpuClient, inceptorClient, namespace, inceptorInstanceName, executorPodNamePrefix, slaLevel, sparkUiUrl):
    #         return 1

    return 0


def get_inceptor_expansion_or_reduction_script(replicas, namespace, inceptorInstanceName):

    script = "kubectl -n " + namespace + " patch instance " + inceptorInstanceName + " --patch '[{\"op\": \"replace\", \"path\": \"/spec/configs/App/executor/replicas\", \"value\":" + str(replicas) + "}]' --type='json'"
    return script


def expansion_inceptor_executor(namespace, inceptorInstanceName):

    logger.info("=======================================\n")
    logger.info("Begin Expansion: %s/%s Inceptor Executor Replicas From %s Expansion to %s \n" % (
    str(namespace), str(inceptorInstanceName), str(Config.EXECUTOR_MIN_REPLICA), str(Config.EXECUTOR_MAX_REPLICA)))

    script = get_inceptor_expansion_or_reduction_script(str(Config.EXECUTOR_MAX_REPLICA), namespace, inceptorInstanceName)
    logger.info("K8s Expansion Scripts: %s " % (script))
    (status, output) = commands.getstatusoutput(script)
    logger.info("Return Code: %s" % (str(status)))

    if str(status) != str(0):
        logger.error("Return Result: %s" % (str(output)))
        logger.info("%s/%s Inceptor Executor Expansion Fail" % (namespace, inceptorInstanceName))
        raise Exception("K8s Scripts Expansion Fail")

    logger.info("Return Result: %s" % (str(output)))
    logger.info("%s/%s Inceptor Executor Expansion Success, Now Executor Num: %s" %(namespace, inceptorInstanceName, str(Config.EXECUTOR_MAX_REPLICA)))

    logger.info("=======================================\n")


def reduction_inceptor_executor(namespace, inceptorInstanceName, slaLevel):
    replica = int(math.ceil(float(Config.SLA[slaLevel][0]) / 100 * Config.EXECUTOR_MAX_REPLICA))

    if replica < int(Config.EXECUTOR_MAX_REPLICA):
        logger.info("=======================================\n")
        logger.info("Begin Reduction: %s/%s Inceptor Executor Replicas From %s Reduce to %s \n" % (
            namespace, inceptorInstanceName, str(Config.EXECUTOR_MAX_REPLICA), str(replica)))
        logger.info("=======================================\n")

        script = get_inceptor_expansion_or_reduction_script(replica, namespace, inceptorInstanceName)
        logger.info("K8s Reduction Scripts: %s " % (script))
        (status, output) = commands.getstatusoutput(script)
        logger.info("Return Code: %s" % (str(status)))

        if str(status) != str(0):
            logger.error("Return Result: %s" % (str(output)))
            logger.info("%s/%s Inceptor Executor Reduction Fail" % (namespace, inceptorInstanceName))
            raise Exception("K8s Scripts Reduction Fail")

        logger.info("Return Result: %s" % (str(output)))
        logger.info("%s/%s Inceptor Executor Reduction Success, Now Executor Num: %s" % (namespace, inceptorInstanceName, str(Config.EXECUTOR_MIN_REPLICA)))

        logger.info("=======================================\n")
    else:
        logger.info("Reduction replica not less than max replica, skip. SlaLevel:%s, namespace:%s, inceptorInstanceName:%s", str(slaLevel), str(namespace), str(inceptorInstanceName))

def inceptor_executor_usage_percentage(executor_pod_mem_limit, executorPodUsage):
    usageTotal = 0
    limitTotal = 0
    for index in executor_pod_mem_limit.keys():
        limitTotal += executor_pod_mem_limit[index]
        usageTotal += executorPodUsage[index]
    return float(usageTotal/limitTotal)
if __name__ == '__main__':

    logger.info("****************************************************")

    logger.info("Begin Check Time: " + str(time.strftime("%Y-%m-%d %H:%M:%S")) + "\n")
    # logger.info("Namespace: %s " % (str(Config.NAMESPACE)))
    # logger.info("Inceptor Instance Name: %s " % (str(Config.INCEPTOR_INSTANCE_NAME)))
    # logger.info("Inceptor SparkUI Url: %s " % (str(Config.INCEPTOR_SPARK_UI_URL)))
    # logger.info("Promethues Url: %s \n" % (str(Config.PROMETHEUS_URL)))

    memoryClient, cpuClient, inceptorClient = init_client()

    inceptorList = Config.INCEPTOR_LIST
    if len(inceptorList) > 0:
        for inceptorInfo in inceptorList:
            if len(inceptorInfo) == Config.INCEPTOR_INFO_LEN:
                namespace = str(inceptorInfo[0])
                inceptorInstanceName = str(inceptorInfo[1])
                slaLevel = int(inceptorInfo[2])
                sparkUiUrl = str(inceptorInfo[3] + "/api/")
                executorPodNamePrefix = str(inceptorInfo[4])
                predict_num = predict_inceptor_expansion_or_reduction(memoryClient, cpuClient, inceptorClient, namespace, inceptorInstanceName, slaLevel, sparkUiUrl, executorPodNamePrefix)
                if int(predict_num) > 0:
                    logger.info("%s/%s Inceptor Executor Begin Expansion" % (namespace, inceptorInstanceName))
                    expansion_inceptor_executor(namespace, inceptorInstanceName)
                elif int(predict_num) < 0:
                    logger.info("%s/%s Inceptor Executor Begin Reduction" % (namespace, inceptorInstanceName))
                    reduction_inceptor_executor(namespace, inceptorInstanceName, slaLevel)
                else:
                    logger.info("%s/%s Inceptor Executor Does not Need Change" % (namespace, inceptorInstanceName))
            else:
                logger.error("Inceptor info not compelete, Please check! %s", str(inceptorInfo))

    logger.info("****************************************************\n")
