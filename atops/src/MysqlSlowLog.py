# encoding: utf-8
import os
import configparser
import time
import paramiko
import pymysql
import json
import requests
import datetime
import InfoMonConstants
from LoggerHelper import LoggerHelper


def getlogpath(host, database):
    # 连接数据库
    connect = pymysql.Connect(
        host=host, port=int(database['port']), user=database['user'],
        passwd=database['password'], db=database['dbname'], charset='utf8')
    # 获取游标
    cursor = connect.cursor()
    slowlogstatus = "show variables like 'slow_query_log'"
    cursor.execute(slowlogstatus)
    row = cursor.fetchone()
    if row[1] == "OFF":
        return None
    sql = "show variables like 'slow_query_log_file'"
    cursor.execute(sql)
    row = cursor.fetchone()
    cursor.close()
    return row[1]


class MysqlSlowLog:
    # 初始文件的偏移量
    current_offset = 0
    current_header = 0
    analyzetext = ''

    def __init__(self, system, host):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(host, 22, username=system['user'],
                         password=system['password'], timeout=5)
        self.logger = LoggerHelper('MysqlSlowLog')

    def getMysqlslowlogconf(self):
        pwd = os.getcwd()
        config = configparser.ConfigParser()
        config.read(InfoMonConstants.INIFILE, encoding='utf-8')
        seek = config.get('mysqlslowlog', 'seek')
        header = config.get('mysqlslowlog', 'header')
        return seek, header

    def writeMysqlslowseekconf(self, seek):
        pwd = os.getcwd()
        config = configparser.ConfigParser()
        config.read(InfoMonConstants.INIFILE, encoding='utf-8')
        config.set('mysqlslowlog', 'seek', str(seek))
        config.write(open(InfoMonConstants.INIFILE, 'w'))

    def writeMysqlslowheaderconf(self, header):
        pwd = os.getcwd()
        config = configparser.ConfigParser()
        config.read(InfoMonConstants.INIFILE, encoding='utf-8')
        config.set('mysqlslowlog', 'header', str(header))
        config.write(open(InfoMonConstants.INIFILE, 'w'))

    def closessh(self):
        self.ssh.close()

    def executeCMD(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        return stdout

    @staticmethod
    def writefile(info, tmpfile):
        f = open(tmpfile, 'a+')
        f.writelines(info+'\n')
        f.close()

    def getslowlog(self, logfile, tmpfile):
        # 开启了慢日志进行分析
        sftp_client = self.ssh.open_sftp()
        remote_file = sftp_client.open(logfile, 'r')
        # 获取开始的文件offset偏移量和header头部位置
        self.current_offset, self.current_header = self.getMysqlslowlogconf()
        # 判断文件头部是否第一次出现
        fristline = remote_file.readline()
        # 获取第二行进行判断
        fristline = remote_file.readline()
        firstlineLen = len(fristline)
        firstheader = str(fristline.strip())
        if (firstheader == self.current_header):
            self.logger.info("如果历史的头部和配置文件的头部一样，则继续配置文件的seek位置，开始增量获取日志信息")
            remote_file.seek(int(self.current_offset))
            line = remote_file.readline()
            while line:
                self.current_offset = int(self.current_offset) + len(line)
                self.writefile(line.strip(), tmpfile)
                line = remote_file.readline()
            # 只更新配置文件的seek值
            self.writeMysqlslowheaderconf(self.current_offset)
            self.writeMysqlslowseekconf(self.current_offset)
        else:
            self.logger.info("如果文件header有改变， 那么先删除本地文件后进行全量日志收集")
            if (os.path.exists(tmpfile)):
                os.remove(tmpfile)
            remote_file.seek(0)
            line = remote_file.readline()
            self.current_offset = 0
            while line:
                self.current_offset = self.current_offset + len(line)
                self.writefile(line.strip(), tmpfile)
                line = remote_file.readline()
            # 更新配置文件信息
            self.writeMysqlslowheaderconf(firstheader)
            self.writeMysqlslowseekconf(self.current_offset)

        remote_file.close()

    def pt_query_digest(self, inputSlowlog, outputFilename, host, database, endpoint):
        # 分析最近一小时的慢日志
        os.popen("chmod 777 {}".format(inputSlowlog))
        untiltime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sincetime = (datetime.datetime.now() -
                     datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
        cmdstr = "pt-query-digest " + inputSlowlog + " --since \'" + \
            sincetime + "\' --until \'"+untiltime + "\'"
        result = os.popen(cmdstr)
        data = {
            "ip": host,
            "step": 60,
            "values": [{
                "sid": database['dbname'],
                "alert_time": '',
                "alert_code": '',
                "details": result.read()
            }],
            "log_type": 'mysql_slow',
            "timestamp": int(time.time()),
            "metric": 'alter_log',
            "endpoint": endpoint,
            "db_type": database['database_type']
        }
        self.send(data)

    def send(self, data):
        url = InfoMonConstants.EXTEND_CLIENT + '/alert_log'
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


def getMysqlSlowLog(endpoint, connectInfo):
    host = connectInfo['host']
    database = connectInfo['database_info']
    system = connectInfo['system_info']
    logfile = getlogpath(host, database)
    tmpfile = os.path.join(
        os.getcwd(), 'tmp', '{}_slowlog.log'.format(endpoint))
    slowlog = MysqlSlowLog(system, host)
    if logfile is not None:
        targetDir = database.get('target_directory', '/tmp')
        slowlog.getslowlog(logfile, tmpfile)
        slowlog.pt_query_digest(tmpfile, os.path.join(
            targetDir, '{}_slowlog.log'.format(endpoint)), host, database, endpoint)


if __name__ == '__main__':
    slowlog = MysqlSlowLog()
    # 测试远程取慢日志到本地
    slowlog.getslowlog(getlogpath())
    # 测试分析慢日志
    slowlog.pt_query_digest('/home/mysql/slowlog.log', '/tmp/result.log')
