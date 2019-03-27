# encoding: utf-8
'''
Created on 2018年11月6日
description:数据收集器
@author: liwenhui
'''
import os
import time
import InfoMonConstants
from ConnectionHelper import MysqlConnectionHelper
from Collector import Collector


class MysqlCollector(Collector):
    delaySQL = "SELECT schema_name, SUM(count_star) count, \
        ROUND((SUM(sum_timer_wait)/SUM(count_star))/1000000000) AS avg_ms \
        FROM performance_schema.events_statements_summary_by_digest \
        WHERE schema_name IS NOT NULL and schema_name = '{0}' GROUP BY schema_name"

    def __init__(self, endpoint, connectInfo):
        super(MysqlCollector, self).__init__(endpoint, connectInfo, 'MysqlCollector')

    def getTopSQL(self):
        '''
        收集top sql数据
        '''
        with self.errorHandle():
            res = []
            sql = 'select schema_name,digest_text,count_star,sum_timer_wait/1000000 "sum_total_time_ms",\
                min_timer_wait/1000000 "min_time_ms",AVG_TIMER_WAIT/1000000 "avg_time_ms",\
                MAX_TIMER_WAIT/1000000 "max_time_ms",sum_lock_time/1000000 "lock_time_ms",\
                sum_errors,sum_warnings,sum_rows_affected,sum_rows_sent,sum_rows_examined,sum_created_tmp_tables,\
                sum_created_tmp_disk_tables,SUM_SORT_RANGE,SUM_SORT_ROWS,SUM_NO_INDEX_USED,SUM_NO_GOOD_INDEX_USED \
                from performance_schema.events_statements_summary_by_digest where digest_text not like \'SHOW %\' \
                and digest_text not like \'SET %\' and digest_text not like \'SELECT @@%\' and digest_text not like \'COMMIT%\' and digest_text not like \'USE %\''
            results = self.connHelper.execute(sql)
            for line in results:
                res.append({
                    "schema_name": line[0],
                    "digest_text": line[1],
                    "count_star": line[2],
                    "sum_total_time_ms": int(line[3]),
                    "min_time_ms": int(line[4]),
                    "avg_time_ms": int(line[5]),
                    "max_time_ms": int(line[6]),
                    "lock_time_ms": int(line[7]),
                    "sum_errors": line[8],
                    "sum_warnings": line[9],
                    "sum_rows_affected": line[10],
                    "sum_rows_sent": line[11],
                    "sum_rows_examined": line[12],
                    "sum_created_tmp_tables": line[13],
                    "sum_created_tmp_disk_tables": line[14],
                    "sum_sort_range": line[15],
                    "sum_sort_rows": line[16],
                    "sum_no_index_used": line[17],
                    "sum_no_good_index_used": line[18]
                })
            self.topSQL = {
                "endpoint": self.endpoint,
                "sid": self.dbInfo['dbname'],
                "metric": "top_sql",
                "timestamp": self.ts,
                "step": 60,
                "values": res,
                "counterType": "GAUGE",
                "type": self.dbInfo['database_type'],
                "tags": ""
            }

    def initConnectionAndRun(self, fun):
        '''
        初始化数据库连接
        '''
        try:
            self.connHelper = MysqlConnectionHelper(self.host, self.dbInfo)
        except Exception as e:
            self.delay = -1
            self.logger.error(str(e))
        else:
            fun()
            result = self.connHelper.execute(self.delaySQL.format(self.dbInfo['dbname']))
            self.delay = float(result[0][2])
            self.connHelper.conn.close()

    def getFuncList(self):
        threadFunList = [
            self.getInnodbCacheRatio, self.getInnodbBufferUsage, self.getAvgResponseTime, self.getBytesReceived, self.getBytesSend, 
            self.getInnodbDataReads, self.getInnodbDataWrites, self.getInnodbDataFsyncs, self.getTps, self.getQps,
            self.getInnodbDataRead, self.getInnodbDataWritten, self.getInnodbOsLogFsyncs, self.getInnodbOsLogWritten, 
            self.getInnodbLogWrites, self.getInnodbLogWriteRequests, self.getInnodbRowDeleted, self.getInnodbRowsInserted, self.getInnodbRowsRead, self.getInnodbRowsUpdated,
            self.getKeyReadRequests, self.getKeyWriteRequests, self.getKeyReads, self.getKeyWrites, self.getThreadsRunning, self.getUsedSize,
            self.getTopNCpu, self.getTopNMemory, self.getTopNNetwork, self.getTopNDisk
        ]

        return threadFunList

    def getInnodbCacheRatio(self):
        '''
        innodb缓存命中率
        '''
        results = self.connHelper.execute(
            "select phy_read.count,total_read.count,1-phy_read.count/total_read.count \
            from (select count from information_schema.INNODB_METRICS t \
            where t.name =  'buffer_pool_reads') phy_read,\
            (select count from information_schema.INNODB_METRICS t\
            where t.name =  'buffer_pool_read_requests') total_read")
        for line in results:
            data = [
                ('innodb_buffer_read_phyRead', line[0], "name=bufferHitRatio"),
                ('innodb_buffer_read_totalRead', line[1], "name=bufferHitRatio"),
                ('innodb_buffer_read_hit_ratio', round(float(line[2]), 5), "name=bufferHitRatio"),
            ]
            self.addNewItems(data)

    def getInnodbBufferUsage(self):
        '''
        innodb缓存池利用率
        '''
        results = self.connHelper.execute(
            "select buffer_free.count,buffer_total.count,1-buffer_free.count/buffer_total.count\
            from (select count from information_schema.INNODB_METRICS t\
            where t.name =  'buffer_pool_pages_free') buffer_free,\
            (select count from information_schema.INNODB_METRICS t\
            where t.name =  'buffer_pool_pages_total') buffer_total")
        for line in results:
            data = [
                ('innodb_buffer_page_free', int(line[0]), "name=buffer_usage_ratio"),
                ('innodb_buffer_page_total', int(line[1]), "name=buffer_usage_ratio"),
                ('innodb_buffer_page_usage_Ratio', float(line[2]), "name=buffer_usage_ratio"),
            ]
            self.addNewItems(data)

    def getAvgResponseTime(self):
        '''
        平均响应时间avg_response_time
        '''
        results = self.connHelper.execute(
            "SELECT schema_name, SUM(count_star) count, \
            ROUND((SUM(sum_timer_wait)/SUM(count_star))/1000000000) AS avg_ms \
            FROM performance_schema.events_statements_summary_by_digest\
            WHERE schema_name IS NOT NULL GROUP BY schema_name")
        for line in results:
            self.addNewItem("%s_response_count" % line[0], float(line[1]), "name=response_time_ms")
            self.addNewItem("%s_response_time_ms" % line[0], float(line[2]), "name=response_time_ms")

    def getBytesReceived(self):
        '''
        每秒接受字节数bytes_received
        '''
        BytesReceivedSql = "show global status like 'Bytes_received'"
        old_result = self.connHelper.getValue(BytesReceivedSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(BytesReceivedSql)
        self.addNewItem("bytesReceived", new_result - old_result, "name=avg_bytesReceived")

    def getBytesSend(self):
        '''
        每秒发送的字节数bytes_sent
        '''
        BytesSendSql = "show global status like 'Bytes_sent'"
        old_result = self.connHelper.getValue(BytesSendSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(BytesSendSql)
        self.addNewItem("bytesSend", new_result - old_result, "name=avg_bytesSend")

    def getInnodbDataReads(self):
        '''
        平均每秒从文件中读取的次数
        '''
        InnodbDataRead = "show global status like 'Innodb_data_reads'"
        old_result = self.connHelper.getValue(InnodbDataRead)
        time.sleep(1)
        new_result = self.connHelper.getValue(InnodbDataRead)
        self.addNewItem("Innodb_data_read_count", new_result - old_result, "name=Innodb_data_read_count")

    def getInnodbDataWrites(self):
        '''
        平均每秒从文件中写入的次数innodb_data_writes
        '''
        InnodbDataWrite = "show global status like 'Innodb_data_writes'"
        old_result = self.connHelper.getValue(InnodbDataWrite)
        time.sleep(1)
        new_result = self.connHelper.getValue(InnodbDataWrite)
        self.addNewItem("Innodb_data_write_count", new_result - old_result, "name=Innodb_data_write_count")

    def getInnodbDataFsyncs(self):
        '''
        innodb平均每秒进行fsync() 操作的次数Innodb_data_fsyncs
        '''
        InnodbDataFsyncs = "show global status like 'Innodb_data_fsyncs'"
        old_result = self.connHelper.getValue(InnodbDataFsyncs)
        time.sleep(1)
        new_result = self.connHelper.getValue(InnodbDataFsyncs)
        self.addNewItem("Innodb_data_fsyncs", new_result - old_result, "name=Innodb_data_fsyncs")

    def getTps(self):
        '''
        每秒事务数tps
        '''
        tpsCommit = "show global status like 'Com_commit'"
        tpsRollback = "show global status like 'Com_rollback'"
        old_commitresult = self.connHelper.getValue(tpsCommit)
        old_rollbackresult = self.connHelper.getValue(tpsRollback)
        old_tps = old_commitresult + old_rollbackresult
        time.sleep(1)
        new_commitresult = self.connHelper.getValue(tpsCommit)
        new_rollbackresult = self.connHelper.getValue(tpsRollback)
        new_tps = new_commitresult + new_rollbackresult
        self.addNewItem("tps_per_second", new_tps - old_tps, "name=tps_per_second")

    def getQps(self):
        '''
        每秒查询qps
        '''
        qpsSql = "show global status like 'Questions'"
        old_result = self.connHelper.getValue(qpsSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(qpsSql)
        self.addNewItem("qps_per_second", new_result - old_result, "name=qps_per_second")

    def getInnodbDataRead(self):
        '''
        平均每秒钟读取的数据量，单位为KB
        '''
        innodbDataReadSql = "show global status like 'Innodb_data_read'"
        old_result = self.connHelper.getValue(innodbDataReadSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(innodbDataReadSql)
        self.addNewItem("data_read_per_second", new_result - old_result, "name=data_read_per_second")

    def getInnodbDataWritten(self):
        '''
        平均每秒钟写入的数据量，单位为KB，innodb_data_written
        '''
        innodbDataWriteenSql = "show global status like 'Innodb_data_written'"
        old_result = self.connHelper.getValue(innodbDataWriteenSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(innodbDataWriteenSql)
        self.addNewItem("data_written_per_second", new_result - old_result, "name=data_written_per_second")

    def getInnodbOsLogFsyncs(self):
        '''
        平均每秒向日志文件完成的fsync()写数量，innodb_os_log_fsyncs
        '''
        innodbOsLogFsyncs = "show global status like 'Innodb_os_log_fsyncs'"
        old_result = self.connHelper.getValue(innodbOsLogFsyncs)
        time.sleep(1)
        new_result = self.connHelper.getValue(innodbOsLogFsyncs)
        self.addNewItem("innodb_log_fsyncs", new_result - old_result, "name=innodb_log_fsyncs")

    def getInnodbOsLogWritten(self):
        '''
        平均每秒写入日志文件的字节数，innodb_os_log_written
        '''
        InnodbOsLogWrittenSql = "show global status like 'Innodb_os_log_written'"
        old_result = self.connHelper.getValue(InnodbOsLogWrittenSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(InnodbOsLogWrittenSql)
        self.addNewItem("innodb_os_log_written", new_result - old_result, "name=innodb_os_log_written")

    def getInnodbLogWrites(self):
        '''
        平均每秒向日志文件的物理写次数, Innodb_log_writes
        '''
        InnodbLogWritesSql = "show global status like 'Innodb_log_writes'"
        old_result = self.connHelper.getValue(InnodbLogWritesSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(InnodbLogWritesSql)
        self.addNewItem("innodb_log_writes", new_result - old_result, "name=innodb_log_writes")

    def getInnodbLogWriteRequests(self):
        '''
        平均每秒日志写请求数
        '''
        InnodbLogWriteRequestSql = "show global status like 'Innodb_log_write_requests'"
        old_result = self.connHelper.getValue(InnodbLogWriteRequestSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(InnodbLogWriteRequestSql)
        self.addNewItem("Innodb_log_write_requests", new_result - old_result, "name=Innodb_log_write_requests")

    def getInnodbRowDeleted(self):
        '''
        平均每秒从innodb表删除的行数
        '''
        InnodbRowDeletedSql = "show global status like 'Innodb_rows_deleted'"
        old_result = self.connHelper.getValue(InnodbRowDeletedSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(InnodbRowDeletedSql)
        self.addNewItem("Innodb_rows_deleted", new_result - old_result, "name=Innodb_rows_deleted")

    def getInnodbRowsInserted(self):
        '''
        平均每秒从innodb表插入的行数, Innodb_rows_inserted
        '''
        InnodbRowsInsertedSql = "show global status like 'Innodb_rows_inserted'"
        old_result = self.connHelper.getValue(InnodbRowsInsertedSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(InnodbRowsInsertedSql)
        self.addNewItem("Innodb_rows_inserted", new_result - old_result, "name=Innodb_rows_inserted")

    def getInnodbRowsRead(self):
        '''
        平均每秒从innodb表读取的行数, Innodb_rows_read
        '''
        InnodbRowsReadSql = "show global status like 'Innodb_rows_read'"
        old_result = self.connHelper.getValue(InnodbRowsReadSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(InnodbRowsReadSql)
        self.addNewItem("Innodb_rows_read", new_result - old_result, "name=Innodb_rows_read")

    def getInnodbRowsUpdated(self):
        '''
        平均每秒从innodb表更新的行数, Innodb_rows_updated
        '''
        InnodbRowsUpdatedSql = "show global status like 'Innodb_rows_updated'"
        old_result = self.connHelper.getValue(InnodbRowsUpdatedSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(InnodbRowsUpdatedSql)
        self.addNewItem("Innodb_rows_updated", new_result - old_result, "name=Innodb_rows_updated")

    def getKeyReadRequests(self):
        '''
        MyISAM平均每秒钟从缓冲池中的读取次数, key_read_requests
        '''
        KeyReadRequestsSql = "show global status like 'key_read_requests'"
        old_result = self.connHelper.getValue(KeyReadRequestsSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(KeyReadRequestsSql)
        self.addNewItem("myisam_read_count", new_result - old_result, "name=myisam_read_count")

    def getKeyWriteRequests(self):
        '''
        MyISAM平均每秒钟从缓冲池中的写入次数, Key_write_requests
        '''
        KeyWriteRequestsSql = "show global status like 'Key_write_requests'"
        old_result = self.connHelper.getValue(KeyWriteRequestsSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(KeyWriteRequestsSql)
        self.addNewItem("myisam_write_count", new_result - old_result, "name=myisam_write_count")

    def getKeyReads(self):
        '''
        MyISAM平均每秒钟从硬盘上读取的次数,key_reads
        '''
        KeyReadSql = "show global status like 'key_reads'"
        old_result = self.connHelper.getValue(KeyReadSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(KeyReadSql)
        self.addNewItem("myisam_disk_read_count", new_result - old_result, "name=myisam_disk_read_count")

    def getKeyWrites(self):
        '''
        MyISAM平均每秒钟从硬盘上写入的次数
        '''
        KeyWritesSql = "show global status like 'key_writes'"
        old_result = self.connHelper.getValue(KeyWritesSql)
        time.sleep(1)
        new_result = self.connHelper.getValue(KeyWritesSql)
        self.addNewItem("myisam_disk_write_count", new_result - old_result, "name=myisam_disk_write_count")

    def getThreadsRunning(self):
        '''
        innodb引擎的并发数， threads_running
        '''
        ThreadsRuningSql = "show global status like 'threads_running'"
        result = self.connHelper.getValue(ThreadsRuningSql)
        self.addNewItem("Innodb_Thread_Running", result, "name=Innodb_Thread_Running")

    def getUsedSize(self):
        sql = 'select (select round(sum(DATA_LENGTH/1024/1024),2) from information_schema.TABLES) as "data(MB)", \
            (select VARIABLE_VALUE from information_schema.GLOBAL_VARIABLES WHERE VARIABLE_NAME = \'DATADIR\') as "datadir" from dual'
        results = self.connHelper.execute(sql)

        datadirDict = {}
        for result in results:
            if datadirDict.has_key(result[1]):
                datadirDict[result[1]] = datadirDict[result[1]] + \
                    float(result[0])
            else:
                datadirDict[result[1]] = float(result[0])

        if len(datadirDict) > 0:
            for key, value in datadirDict.items():
                metric = key.replace('/', '_')
                self.addNewItem("tablespace%susedmsize" % metric, value)
            self.send(
                InfoMonConstants.EXTEND_CLIENT + '/tablespace',
                {
                    "endpoint": self.endpoint,
                    "tablespace": datadirDict.keys(),
                    "db_type": self.dbInfo['database_type']
                })

    def getTopNCpu(self):
        '''
        topn cpu收集
        '''
        result = self.runSystemCMD(
            "ps aux | grep mysql | awk '{print $3}' |  awk '{sum+=$1}END{print sum}'")
        cpuNum = self.runSystemCMD(
            "cat /proc/cpuinfo | grep 'physical id' | sort | uniq | wc -l")
        if result == '':
            return
        result = result.split("\n")
        self.addNewItem('cpu_total_usage', float(result[0])/int(cpuNum))

    def getTopNMemory(self):
        '''
        topn 内存收集
        '''
        result = self.runSystemCMD(
            "ps aux | grep mysql | awk '{print $4}' |  awk '{sum+=$1}END{print sum}'")
        if result == '':
            return
        result = result.split("\n")
        self.addNewItem('memory_total_usage', float(result[0]))

    def getTopNNetwork(self):
        '''
        topn 网络收集
        '''
        netWorkReceSql = "show global status like 'Bytes_received'"
        netWorkSentSql = "show global status like 'Bytes_sent'"
        received = self.connHelper.execute(netWorkReceSql)
        sent = self.connHelper.execute(netWorkSentSql)
        time.sleep(1)
        receivednew = self.connHelper.execute(netWorkReceSql)
        sentnew = self.connHelper.execute(netWorkSentSql)
        result = int(receivednew[0][1]) + int(sentnew[0][1])-int(received[0][1]) - int(sent[0][1])
        self.addNewItem('network_traffic_per_sec', result)

    def getTopNDisk(self):
        '''
        topn 磁盘收集
        '''
        diskReadSql = "show global status like 'Innodb_data_read'"
        diskWriteSql = "show global status like 'innodb_data_written'"
        diskRead = self.connHelper.execute(diskReadSql)
        dislWrite = self.connHelper.execute(diskWriteSql)
        time.sleep(1)
        diskReadnew = self.connHelper.execute(diskReadSql)
        dislWritenew = self.connHelper.execute(diskWriteSql)
        result = int(diskReadnew[0][1]) + int(dislWritenew[0][1])- int(diskRead[0][1]) - int(dislWrite[0][1])
        self.addNewItem('disk_io_per_sec', result)
