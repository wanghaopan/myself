# -*- coding: utf-8 -*-
import time

import Config
from Logger import Logger
from util.InceptorUtil import InceptorUtil

logger = Logger(logname=Config.LOGGER_LOC, loglevel=1, logger="InceptorClient").getlog()


class InceptorClient(object):

    def __init__(self):
        pass

    def get_inceptor_jobs_range_time(self, sparkUiUrl, start_time):

        start_time = time.strftime("%Y%m%d%H%M%S", time.localtime(float(start_time)))

        data_list = InceptorUtil.get_inceptor_jobs(sparkUiUrl, start_time)

        if len(data_list) == 0:
            logger.debug("Get Inceptor Jobs Is Empty")
            return {}

        data_dict = {}
        for data in data_list:
            data_dict[str(data['jobId'])] = str(data['status'])

        return data_dict

    def get_inceptor_stages_range_time(self, sparkUiUrl, start_time):

        start_time = time.strftime("%Y%m%d%H%M%S", time.localtime(float(start_time)))

        data_list = InceptorUtil.get_inceptor_stages(start_time, sparkUiUrl)

        if len(data_list) == 0:
            logger.warn("Get Inceptor Stages Is Empty")
            return {}

        data_dict = {}
        for data in data_list:
            data_dict[str(data['stageId'])] = str(data['status'])

        return data_dict

    def get_inceptor_running_jobs(self, sparkUiUrl):

        data_list = InceptorUtil.get_inceptor_status_jobs("running", sparkUiUrl)

        if len(data_list) == 0:
            logger.debug("Get Inceptor Running Jobs Is Empty")
            return {}

        data_dict = {}
        for data in data_list:
            data_dict[str(data['jobId'])] = str(data['status'])

        return data_dict

    def get_inceptor_active_stages(self, sparkUiUrl):

        data_list = InceptorUtil.get_inceptor_status_stages("active", sparkUiUrl)

        if len(data_list) == 0:
            logger.debug("Get Inceptor Stages Is Empty")
            return {}

        data_dict = {}
        for data in data_list:
            data_dict[str(data['stageId'])] = str(data['status'])

        return data_dict

    def get_inceptor_pending_stages(self, sparkUiUrl):

        data_list = InceptorUtil.get_inceptor_status_stages("pending", sparkUiUrl)

        if len(data_list) == 0:
            logger.debug("Get Inceptor Jobs Is Empty")
            return {}

        data_dict = {}
        for data in data_list:
            data_dict[str(data['stageId'])] = str(data['status'])

        return data_dict

    def get_inceptor_executors(self, sparkUiUrl):

        data_list = InceptorUtil.get_inceptor_executors(sparkUiUrl)

        if len(data_list) == 0:
            logger.debug("Get Inceptor Executor Is Empty")
            return {}

        data_dict = {}
        for data in data_list:
            if str(data['execId']).__contains__("driver"):
                continue
            data_dict[str(data['execId'])] = {"totalShuffleWrite": str(data['totalShuffleWrite']),
                                              "totalShuffleRead": str(data['totalShuffleRead']),
                                              "totalGCTime": str(data['totalGCTime']),
                                              "totalCores": str(data['totalCores']),
                                              "maxMemory": str(data['maxMemory']),
                                              "activeTasks": str(data['activeTasks'])}

        return data_dict


if __name__ == '__main__':

    InceptorClient = InceptorClient()

    start_time = time.time() - 60 * 60 * 3

    id_server_dict = InceptorClient.get_inceptor_jobs_range_time(sparkUiUrl, start_time)
    print id_server_dict

    id_server_dict = InceptorClient.get_inceptor_stages_range_time(start_time, sparkUiUrl)
    print id_server_dict

    id_server_dict = InceptorClient.get_inceptor_running_jobs(sparkUiUrl)
    print id_server_dict

    id_server_dict = InceptorClient.get_inceptor_active_stages(sparkUiUrl)
    print id_server_dict

    id_server_dict = InceptorClient.get_inceptor_pending_stages(sparkUiUrl)
    print id_server_dict

    id_server_dict = InceptorClient.get_inceptor_executors(inceptorSparkUiUrl)
    print id_server_dict



