# -*- coding: utf-8 -*-

import Config
from Logger import Logger
from util.DbaServiceUtil import DbaServiceUtil

logger = Logger(logname=Config.LOGGER_LOC, loglevel=1, logger="DbaServiceClient").getlog()


class DbaServiceClient(object):

    def __init__(self):
        pass

    def get_all_inceptor_servers(self):

        data_dict = DbaServiceUtil.get_inceptor_servers()

        if len(data_dict) == 0:
            logger.error("Get Inceptor Servers Is Empty")
            raise Exception("Get Inceptor Servers Is Empty")

        id_server_dict = dict()

        for val in data_dict.values():
            tmp_id = str(val['serverId'])
            tmp_key = str(val['key'])
            timestamp = str(val['timestamp'])
            id_server_dict[tmp_id] = {"timestamp": timestamp, "server": tmp_key}

        return id_server_dict

    def get_one_inceptor_server_overview(self, service_id):

        data_dict = DbaServiceUtil.get_one_inceptor_server_overview(service_id)

        if len(data_dict) == 0:
            logger.error("Get One Inceptor Servers Is Empty")
            raise Exception("Get One Inceptor Servers Is Empty")

        return data_dict

    # def get_one_inceptor_server_executor_num(self, service_id):
    #     data_dict = InceptorUtil.get_one_inceptor_server_overview(service_id)
    #     return int(str(data_dict['executors']))

    def get_lastest_inceptor_server_id(self):

        data_dict = DbaServiceUtil.get_inceptor_servers()

        if len(data_dict) == 0:
            logger.error("Get Inceptor Servers Is Empty")
            raise Exception("Get Inceptor Servers Is Empty")

        max_timestamp = 0.0
        inceptor_server_id = ""
        for val in data_dict.values():
            timestamp = str(val['timestamp'])
            if float(timestamp) > float(max_timestamp):
                max_timestamp = timestamp
                inceptor_server_id = str(val['key'])

        return inceptor_server_id


if __name__ == '__main__':

    InceptorClient = DbaServiceClient()

    id_server_dict = InceptorClient.get_lastest_inceptor_server_id()
    print id_server_dict

    id_server_dict = InceptorClient.get_all_inceptor_servers()
    print id_server_dict

    data_dict = InceptorClient.get_one_inceptor_server_overview(id_server_dict.values()[0])
    print data_dict

    executor_num = InceptorClient.get_one_inceptor_server_executor_num(id_server_dict.values()[0])
    print executor_num





