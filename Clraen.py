# -*- coding: utf-8 -*-
"""
-------------------------------------------------
# @Project      :数据清洗
# @File         :累计流量数据清洗算法
# @Date         :2020/12/10 14:00
# @Py_version   :Py_3.7
# @Author       :Luoters
# @Email        :118****139@qq.com
# @Software     :PyCharm
-------------------------------------------------
"""
import cx_Oracle
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine
from sqlalchemy.dialects.oracle import \
    BFILE, BLOB, CHAR, CLOB, DATE, \
    DOUBLE_PRECISION, FLOAT, INTERVAL, LONG, NCLOB, \
    NUMBER, NVARCHAR, NVARCHAR2, RAW, TIMESTAMP, VARCHAR, \
    VARCHAR2
import matplotlib.pyplot as plt
import os

os.environ['NLS_LANG']='SIMPLIFIED CHINESE_CHINA.UTF8'

#连接数据库、获取数据
def connectToOracle():
    conn=create_engine('oracle+cx_oracle://用户名:密码@IP地址:端口号/实例名')      #conn连接器

    # #测试sql
    # sql="select MONITOR_ID,COLLECT_DATE,COLLECT_TIME,VALUE_DATA,UPLOAD,LESSEE_ID,rk " \
    #     "from (" \
    #         "select a.*,row_number() over(partition by MONITOR_ID order by COLLECT_TIME) rk " \
    #         "from MONITOR_DATA_HIS_202004_副表  a " \
    #     ")" \
    #     "where rk>=0"

    sql="select datata.* from (" \
        "select MONITOR_ID, " \
            "to_date(substr(to_char(COLLECT_DATE,'yyyy-mm-dd hh:mi:ss'),0,10),'yyyy-mm-dd') COLLECT_DATE," \
            "COLLECT_DATE COLLECT_TIME," \
            "round(VALUE_DATA,2) VALUE_DATA," \
            "UPLOAD," \
            "LESSEE_ID," \
            "row_number() over(partition by MONITOR_ID order by COLLECT_DATE) rk " \
        "from 表名" \
        "where MONITOR_ID in(" \
            "select MONITOR_ID from (" \
                "select b.MONITOR_ID MONITOR_ID " \
                "from (" \
                    "select distinct MONITOR_ID from monitor_data_relation t1 join monitor_item t2 " \
                    "on t1.item_id=t2.item_id " \
                    "where item_name!='负累计流量' and item_name!='反向累计流量' and item_name like '%累计流量%'" \
                ") a " \
            "left join" \
                "(SELECT distinct MONITOR_ID FROM 表名) b " \
            "on a.MONITOR_ID=b.MONITOR_ID" \
            ") where MONITOR_ID is not null " \
        ") " \
    ") datata " \
    "where rk>=1 and VALUE_DATA!=0"

    try:
        data=pd.read_sql(sql,conn)
        print("连接成功")
    except:
        print("connecterror")
    conn.dispose()      #关闭连接器

    MDH_Data_Cleansing(data)

#数据清洗
def MDH_Data_Cleansing(data):
    print("待清洗数据：\n", data)
    #print(data.isnull())
    data.info()     #查看数据基本信息
    rk=np.array(data['rk'])     #声明所有递增趋势序号的数组，rk数据模板：[1,2,3,1,2,1,2,3,4···]
    #print(rk)
    m=len(rk)

    #获取每个递增趋势数据的头节点的行索引，存进节点数组arraySwitch
    arraySwitch = []
    for j in range(1,m-1):
        if(rk[j] == 1):
            arraySwitch.append(j-1)
            arraySwitch.append(j)
        j += 1

    arraySwitch.insert(0,0)
    arraySwitch.append(m-1)       #追加最后一个递增趋势数据的尾节点的行索引（即所有递增趋势的数据的总数-1）
    num=len(arraySwitch)
    print("节点数组个数num=",num)
    print("节点数组arraySwitch=",arraySwitch)

    # 对data数组中不符合单调递增趋势的异常数据进行清洗
    dataResult=pd.DataFrame(columns=['value_data'])       #用于存储最终清洗和降噪完成，合并后的Dframe数据
    s=0
    while s < num-1:
        value_data1 = np.array(data.loc[arraySwitch[s]:arraySwitch[s+1],'value_data'])      #循环逐一取出递增趋势数据
        #value_data2=value_data1[0:-1]       #删除每一个取出的递增趋势数据的脏数据（每个递增趋势数据的最后一个值，但最后一个递增趋势数据是没有脏数据的）
        #print(value_data1)
        n = len(value_data1)    #获取单个递增趋势数据的个数

        #清洗（以直接前驱进行替换）
        for i in range(1, n - 2):
            if (value_data1[i] < value_data1[i - 1]):
                value_data1[i] = value_data1[i - 1]
            if (value_data1[i] > value_data1[i - 1] and value_data1[i] > value_data1[i + 1] and value_data1[i] > value_data1[i + 2]):
                value_data1[i] = value_data1[i - 1]
            i += 1
        data_demo=pd.DataFrame(value_data1,columns=['value_data'])
        print("\n\n该递增趋势数据,清洗后/降噪前：\n",data_demo)

        # 箱线图分析法检测噪声值
        print("递增趋势数据数据量为",n,"，描述信息：","\n",data_demo['value_data'].describe(percentiles=[.25, .75], include=['object', 'float64']))  # describe
        distance_data = data_demo['value_data'].quantile(0.75) - data_demo['value_data'].quantile(0.25)  # 四分位距，即箱
        top_data = data_demo['value_data'].quantile(0.75) + 1.5 * distance_data  # 箱线的上限
        bottom_data = data_demo['value_data'].quantile(0.25) - 1.5 * distance_data  # 箱线的下限
        count_data = (data_demo['value_data'] <= top_data) | (data_demo['value_data'] >= bottom_data)  # 噪声值

        index_toarray = np.array(data_demo[count_data == False].index)  # 取出异常值索引
        print("正常值(True) vs 噪声值个数(False)：\n", count_data.value_counts(), "噪声值的行索引：", index_toarray)  # 打印噪声值数和索引
        # 噪声值处理
        data_demo.loc[index_toarray, 'value_data'] = data_demo['value_data'].median().round(3)  # 中位数替换
        print("降噪后:\n",data_demo)
        dataResult=dataResult.append(data_demo)      #循环逐一合并递增趋势数组，存储于Dataframe表dataResult
        s += 2

    dataResult.index=range(len(dataResult))     #重建Dataframe索引
    data.drop('rk',axis=1,inplace=True)
    data.drop('value_data', axis=1, inplace=True)  # 删除列value_data
    data['value_data']=dataResult['value_data']       #清洗后列值替换
    print("\n最终数据集：\n",data)
    data.dropna(subset=['value_data'], axis=0, inplace=True)  # 删除列value_data存在缺失值的所在行
    data.drop_duplicates(subset=['monitor_id','collect_date','value_data'],keep='first',inplace=True)      #根据多列进行去重


#转类型
def mapping_data_types(data):       #实现Dataframe字段的类型转换(必转，否则就是给自己挖坑，不要问我是怎么知道的)
    dtypedict = {}
    for i, j in zip(data.columns, data.dtypes):
        if "object" in str(j):
            dtypedict.update({i: VARCHAR(256)})
        if "int" in str(j):
            dtypedict.update({i: NUMBER(12,2)})
        if "date" in str(j):
            dtypedict.update({i: DATE(19)})
    return dtypedict

#写入数据库
def MDH_Dataframe_toOracle(data):       #将Dataframe数据写入ORACLE数据库
    from sqlalchemy import types, create_engine
    conn=create_engine('oracle+cx_oracle://用户名:密码@IP地址:端口号/实例名',encoding='utf-8',echo=True)    #连接器
    from sqlalchemy.dialects.oracle import \
        BFILE, BLOB, CHAR, CLOB, DATE, \
        DOUBLE_PRECISION, FLOAT, INTERVAL, LONG, NCLOB, \
        NUMBER, NVARCHAR, NVARCHAR2, RAW, TIMESTAMP, VARCHAR, \
        VARCHAR2
    #print(conn)
    dtypedict = mapping_data_types(data)    #调用转类型方法mapping_data_types，映射数据类型

    tableName='目标表表名'
    data.to_sql(tableName,con=conn,if_exists='append',dtype=dtypedict,chunksize=None,index=False)
    conn.dispose()



if __name__ == '__main__':
    pd.set_option('display.max_columns', None)  # 控制台完整显示列
    pd.set_option('display.max_rows', 50)  # 行数
    pd.set_option('display.width',500)  # 列数
    pd.set_option('max.colwidth',100)   #列宽

    connectToOracle()
    MDH_Dataframe_toOracle(data)
