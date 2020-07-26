# -*- coding: utf-8 -*-
import json
import time

import Config
from HttpClientUtil import HttpClientUtil
from Logger import Logger

logger = Logger(logname=Config.LOGGER_LOC, loglevel=1, logger="InceptorUtil").getlog()

# INCEPTOR_SPARK_UI_URL = str(Config.INCEPTOR_SPARK_UI_URL + "/api/")


class InceptorUtil(object):

    @staticmethod
    def get_inceptor_jobs(sparkUiUrl, start_time=""):

        if start_time == "":
            query_url = sparkUiUrl + "jobs"
        else:
            query_url = sparkUiUrl + "jobs?sinceTime=" + str(start_time)

        result = HttpClientUtil.doGet(query_url, auth_token="")

        return json.loads(result.text)

    @staticmethod
    def get_inceptor_stages(sparkUiUrl, start_time=""):

        if start_time == "":
            query_url = sparkUiUrl + "stages"
        else:
            query_url = sparkUiUrl + "stages?sinceTime=" + str(start_time)

        result = HttpClientUtil.doGet(query_url, auth_token="")

        return json.loads(result.text)

    @staticmethod
    def get_inceptor_executors(sparkUiUrl):

        query_url = sparkUiUrl + "executors"

        result = HttpClientUtil.doGet(query_url, auth_token="")

        return json.loads(result.text)

    @staticmethod
    def get_inceptor_status_jobs(status, sparkUiUrl):

        query_url = sparkUiUrl + "jobs?status=" + str(status)
        result = HttpClientUtil.doGet(query_url, auth_token="")

        return json.loads(result.text)

    @staticmethod
    def get_inceptor_status_stages(status, sparkUiUrl):

        query_url = sparkUiUrl + "stages?status=" + str(status)
        result = HttpClientUtil.doGet(query_url, auth_token="")

        return json.loads(result.text)


if __name__ == '__main__':

    start_time = time.time() - 60 * 60
    timeArray = time.localtime(float(start_time))
    otherStyleTime = time.strftime("%Y%m%d%H%M%S", timeArray)

    print otherStyleTime
    data_list = InceptorUtil.get_inceptor_jobs()
    print data_list
