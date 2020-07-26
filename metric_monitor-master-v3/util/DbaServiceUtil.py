# -*- coding: utf-8 -*-
import json

import Config
from HttpClientUtil import HttpClientUtil
from Logger import Logger

logger = Logger(logname=Config.LOGGER_LOC, loglevel=1, logger="DbaServiceUtil").getlog()

DBASERVICE_URL = str(Config.INCEPTOR_DBASERVICE_URL + "/api/inceptor/")


class DbaServiceUtil(object):

    @staticmethod
    def get_inceptor_servers():

        query_url = DBASERVICE_URL + "servers"

        result = HttpClientUtil.doGet(query_url, auth_token="")
        result_json = json.loads(result.text)

        return result_json['data']

    @staticmethod
    def get_one_inceptor_server_overview(server_id):

        query_url = DBASERVICE_URL + "overview?dataKey=" + str(server_id)

        result = HttpClientUtil.doGet(query_url, auth_token="")
        result_json = json.loads(result.text)

        return result_json['data']


if __name__ == '__main__':

    data_dict = DbaServiceUtil.get_inceptor_servers()
    print data_dict

    data_dict = DbaServiceUtil.get_one_inceptor_server_overview("Inceptor::inceptor-54jzd-8854c777b-82rbn.tdh602security.pod.transwarp.local::16e477ec-adac-4922-bdb4-4e5ad36e0d90")
    print data_dict

