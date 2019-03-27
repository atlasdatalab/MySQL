# encoding: utf-8
'''
Created on 2018年6月15日
description:数据收集器
@author: zhujianhua
'''

import os
import time
import json
import requests
import InfoMonConstants
from threading import Thread
from contextlib import contextmanager
from LoggerHelper import LoggerHelper


class Collector(object):
    def __init__(self, endpoint, connectInfo, loggerName='Collector'):
        self.delay = 1
        self.topSQL = {}
        self.uploadData = []
        self.endpoint = endpoint
        self.ts = int(time.time())  # 时间戳
        self.host = connectInfo['host']
        self.dbInfo = connectInfo['database_info']
        self.systemInfo = connectInfo['system_info']
        self.logger = LoggerHelper(loggerName)
        self.logger.info('collecting infomation from ' + self.host)

    @contextmanager
    def errorHandle(self):
        try:
            yield
        except Exception as e:
            self.logger.error(str(e))

    def run(self):
        self.initConnectionAndRun(self.collect)
        self.sendDelayInfo()
        self.send(InfoMonConstants.FALCON_CLINENT, self.uploadData)

    def runTopSQL(self):
        self.initConnectionAndRun(self.getTopSQL)
        self.send(InfoMonConstants.EXTEND_CLIENT + '/top_sql', self.topSQL)

    def runAwr(self):
        self.initConnectionAndRun(self.getAWR)
        self.send(InfoMonConstants.EXTEND_CLIENT + '/abnormal_waiting', self.topSQL)

    def runMultExecuePlansSql(self):
        self.initConnectionAndRun(self.getMultExecuePlansSql)
        self.send(InfoMonConstants.EXTEND_CLIENT + '/mult_exec_plans_sql', self.topSQL)
    # 执行长事务收集
    def runLongSession(self):
        self.initConnectionAndRun(self.getLongSession)
        self.send(InfoMonConstants.EXTEND_CLIENT + '/long_session', self.topSQL)

    # 锁等待
    def runLockWait(self):
        self.initConnectionAndRun(self.getLockWait)
        self.send(InfoMonConstants.EXTEND_CLIENT + "/lock_wait", self.topSQL)
        
    def runExceptionJob(self):
        self.initConnectionAndRun(self.getExceptionJob)
        self.send(InfoMonConstants.EXTEND_CLIENT + "/exception_job", self.topSQL)

    def initConnectionAndRun(self):
        pass

    def sendDelayInfo(self):
        if self.delay is not None:
            self.send(
                InfoMonConstants.EXTEND_CLIENT + '/delayed_db',
                {
                    "endpoint": self.endpoint,
                    "timestamp": self.ts,
                    "ip": self.host,
                    "sid": self.dbInfo['dbname'],
                    "delay": self.delay
                })

    def getFuncList(self):
        pass

    def getTopSQL(self):
        pass

    def getMultExecuePlansSql(self):
        pass

    def getAWR(self):
        pass

    def getLongSession(self):
        pass
    
    def getLockWait(self):
        pass
    
    def getExceptionJob(self):
        pass

    def collect(self):
        threads = []
        threadFunList = self.getFuncList()

        for task in threadFunList:
            thread = Thread(target=task)
            threads.append(thread)

        for t in threads:
            t.start()
            
        for t in threads:
            t.join()

    def addNewItems(self, data):
        '''
        添加多条条收集到的数据
        :param: data 收集到的数据
        '''
        for item in data:
            self.addNewItem(item[0], item[1], item[2] or '')
            
    def addNewItem(self, metric, value, tags=''):
        '''
        添加单条收集到的数据
        :param: metric
        :param: value
        :param: tags
        '''
        self.uploadData.append({
            "endpoint": self.endpoint,
            "metric": metric,
            "timestamp": self.ts,
            "step": 60,
            "value": value,
            "counterType": "GAUGE",
            "tags": tags,
        })

    def send(self, url, data):
        '''
        发送数据到transfer
        :param url: 目标地址
        :param data: 待发送数据
        '''
        if 0 != len(data):
            self.logger.info("sending data to " + url)
            try:
                data = json.dumps(data)
            except UnicodeDecodeError as e:
                try:
                    data = json.dumps(data, encoding='latin1')
                except UnicodeDecodeError as e:
                    self.logger.error('Conversion to json format failed' + url)
                    self.logger.error(str(e))

            try:
                r = requests.post(url, data=data)
                self.logger.info(r.text)
            except requests.exceptions.ConnectionError:
                self.logger.error('failed to connect to ' + url)
                self.logger.error('transfer data failed')

    def runSystemCMD(self, cmd):
        '''
        在linux中运行命令
        :param cmd: linux命令
        :return: 命令的结果
        '''
        if self.systemInfo['system_type'] != 'linux':
            self.logger.error("database topN only support linux")
            return ''

        if 'true' == self.systemInfo['remote_monitor'].lower():
            import paramiko
            try:
                self.ssh = paramiko.SSHClient()
                self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                self.ssh.connect(
                    self.host, 22, self.systemInfo['user'], self.systemInfo['password'])
            except Exception as e:
                self.logger.error('failed to connect to ' + self.host)
                self.logger.error(str(e))
                return ''
            else:
                stdout = self.ssh.exec_command(cmd)[1]
                result = stdout.read()
                self.ssh.close()
                return result
        else:
            outPut = os.popen(cmd)
            return outPut.read()
