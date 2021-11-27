# -*- coding: utf-8 -*-
import datetime
import json
import sys
import prestodb

class SpSql(object):

    def __getPrestoCon(self):
        prestoConnection = prestodb.dbapi.connect(
            host='10.251.5.83',
            port=9000,
            user='root',
            catalog='hive',
            schema='default'
        )
        return prestoConnection

    def __getTableInfo(self, tableName):
        conn = self.__getPrestoCon()
        cur = conn.cursor()
        cur.execute('''desc %s''' % tableName)
        res = cur.fetchall()
        self.__closeCon(conn)
        return res

    def __getTableComment(self, tableName):
        conn = self.__getPrestoCon()
        cur = conn.cursor()
        try:
            cur.execute('''show create table {0}'''.format(tableName))
            resMiddle = cur.fetchall()
            tableInfo = resMiddle[0][0].split('\n')
            # print(tableInfo)
            commentAll = ''
            for item in tableInfo:
                if item is not None and item.lower().startswith('comment'):
                    commentAll = item
                    break
            commentList = commentAll.split("'")
            comment = ''
            if len(commentList) >= 1:
                comment = commentList[1]
                comment = comment.replace('\\','\\u')
            comment = comment.encode('utf-8').decode('unicode_escape')
            return comment
        except Exception as e:
            print(e)
            return ''
        finally:
            self.__closeCon(conn)

    # 获取字段最大长度
    def __getMaxLength(self, tableInfoList, pos):
        maxLength = 0
        for item in tableInfoList:
            if len(item[pos]) > maxLength:
                maxLength = len(item[pos])
        return maxLength

    def __colSelect(self, maxLength, colName):
        addLength = 5
        realLength = maxLength + addLength
        colLength = realLength - len(colName)
        colName = colName + ","
        for i in range(colLength):
            colName = colName + " "
        return "t1." + colName

    def __spellPartitionKey(self, sql, partitionKeyList):
        for item in partitionKeyList:
            if item[0] == partitionKeyList[0][0]:
                sql = sql + "\n" + " where t1." + item[0] + " = '${stat_date}'"
            else:
                sql = sql + "\n" + "   and t1." + item[0] + " = '${stat_date}'"
        return sql

    def spellSql(self, tableName):
        tableName = tableName.lower()
        # 结果的格式为[['start_date', 'varchar', '', '记录开始日期'], ['_id', 'varchar', '', ''], ['created_at', 'varchar', '', '']]
        tableInfoList = self.__getTableInfo(tableName)

        colMaxLength = self.__getMaxLength(tableInfoList, 0)

        sql = ''
        for item in tableInfoList:
            colName = item[0]
            colName = self.__colSelect(colMaxLength, colName)

            # 去掉最后一个逗号
            if item[0] == tableInfoList[-1][0]:
                colName = colName.replace(',', ' ')

            colType = item[1]
            if colType == 'varchar':
                colType = 'string'
            colComment = item[3]
            # 如果是第一个，增加select

            sli = colName + '-- ' + colType + "," + colComment
            if item[0] == tableInfoList[0][0]:
                sql = sql + "select " + sli + '\n'
            else:
                sql = sql + "       " + sli + '\n'
        tableComment = self.__getTableComment(tableName)
        sql = sql + "  from " + tableName + " as t1     -- " + tableComment

        if tableName.endswith("chain"):
            sql = sql + "\n" + " where t1.dp in ('active', 'expired')"
        else:
            partitionColList = []
            for item in tableInfoList:
                if item[2] == 'partition key':
                    partitionColList.append(item)
            sql = self.__spellPartitionKey(sql, partitionColList)

        print(sql)

    def __closeCon(self, con):
        try:
            con.close()
        except Exception as e:
            print("关闭presto链接异常", e)


if __name__ == '__main__':
    tableName = sys.argv[1]
    spell = SpSql()
    spell.spellSql(tableName)
