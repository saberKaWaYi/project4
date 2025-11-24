import os

log_dir="logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

import logging
from logging.handlers import RotatingFileHandler

def get_rotating_handler(filename,max_bytes=1024*1024*1024,backup_count=5):
    handler=RotatingFileHandler(
        os.path.join(log_dir,filename),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    formatter=logging.Formatter(
        '%(asctime)s - %(levelname)s - %(module)s.%(funcName)s - %(message)s'
    )
    handler.setFormatter(formatter)
    return handler

connect_log_handler=get_rotating_handler("clickhouse.log")
logging_connect=logging.getLogger("connect")
logging_connect.setLevel(logging.INFO)
logging_connect.addHandler(connect_log_handler)

from clickhouse_driver import Client
import time
import atexit
import pandas as pd

class Connect_Clickhouse:

    def __init__(self,config):
        self.config=config
        self.client=self.login()
        atexit.register(self.close)

    def login(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                client=Client(host=self.config["clickhouse"]["HOST"],port=self.config["clickhouse"]["PORT"],user=self.config["clickhouse"]["USERNAME"],password=self.config["clickhouse"]["PASSWORD"])
                return client
            except Exception as e:
                time.sleep(self.config["connection"]["TIME"])
        logging_connect.error("clickhouse登录失败。")
        raise Exception("clickhouse登录失败。")
    
    def close(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                self.client.disconnect()
                return
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging_connect.error("clickhouse关闭失败。")
        raise Exception("clickhouse关闭失败。")
    
    def query(self,query):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                data,columns=self.client.execute(query,with_column_types=True)
                columns=[col[0] for col in columns]
                data=pd.DataFrame(data,columns=columns).astype(str)
                return data
            except Exception as e:
                time.sleep(self.config["connection"]["TIME"])
                logging_connect.error(e)
        logging_connect.error(f"{query}数据获取失败。")
        raise Exception(f"{query}数据获取失败。")

from method import method

import os
import numpy as np
import matplotlib.pyplot as plt

def fc(dataset,config,name,show=True,save_=False):
    m=method()
    datas=[]
    processed_datas=[]
    processed_datas_diff1=[]
    alerts=[]
    for i in dataset:
        if m.time==None:
            m.push_back_data(i)
            datas.append(int(float(i[1])))
            processed_datas.append(m.median[-1])
            processed_datas_diff1.append(m.variation_diff_median[-1])
            alerts.append(0)
            continue
        add_nums=(int(i[0])-m.time)//60-1
        m.push_back_data(i)
        for j in range(-add_nums-1,-1):
            try:
                datas.append(m.data_list_5_days[j])
            except:
                datas.append(m.median[j])
            processed_datas.append(m.median[j])
            processed_datas_diff1.append(m.variation_diff_median[j])
            alerts.append(0)
        datas.append(m.data_list_5_days[-1])
        processed_datas.append(m.median[-1])
        processed_datas_diff1.append(m.variation_diff_median[-1])
        alerts.append(m.alert)
    plt.figure(figsize=(16,9))
    plt.rcParams["font.sans-serif"]=["SimHei"]
    plt.rcParams["axes.unicode_minus"]=False
    time_points=np.arange(1,len(datas)+1)
    plt.subplot(2,1,1)
    plt.plot(time_points,datas,color='blue',label="原始数据")
    plt.plot(time_points,processed_datas,color="red",label="平滑数据")
    true_indices=[i for i,val in enumerate(alerts) if val]
    true_time=[time_points[i] for i in true_indices]
    true_values=[processed_datas[i] for i in true_indices]
    plt.scatter(true_time,true_values,color='green',zorder=5,label="异常的点")
    plt.axvline(x=7200,color='red',linestyle='--',)
    plt.axhline(y=m.low_limit,color='red',linestyle='--')
    plt.title(f"{config["hostname"]}_{config["interface"]}_{config["begin_time"]}_{config["end_time"]}_{name}")
    plt.grid(True,alpha=0.5);plt.legend()
    plt.subplot(2,1,2)
    plt.plot(time_points,processed_datas_diff1,color='purple',label='变异差分')
    plt.title(f"{config["hostname"]}_{config["interface"]}_{config["begin_time"]}_{config["end_time"]}_{name}")
    plt.grid(True,alpha=0.5);plt.legend()
    plt.tight_layout()
    if not save_:
        if not show:
            pass
        else:
            plt.show()
    else:
        name=f"{config["hostname"]}_{config["interface"]}_{config["begin_time"]}_{config["end_time"]}_{name}_{m.small_window_len}_main.jpg".replace(" ","_").replace(":","_")
        dir_path=os.path.dirname(name)
        if dir_path:
            os.makedirs(dir_path,exist_ok=True)
        plt.savefig(name)
    plt.close()
    return sum(alerts)

class Test:

    def __init__(self,show=False,save=True):
        config={
            "connection":{
                "TIMES":1000,
                "TIME":0.1
            },
            "clickhouse":{
                "HOST":"localhost",
                "PORT":5000,
                "USERNAME":"default",
                "PASSWORD":""
            }
        }
        self.conn=Connect_Clickhouse(config)
        self.show=show
        self.save=save
        self.count=0

    def process(self,data):
        lt=[]
        for i in data:
            if len(lt)==0:
                lt.append(i)
            else:
                if i[0]<=lt[-1][0]:
                    continue
                else:
                    lt.append(i)
        return lt

    def get(self,config):
        sql=f'''
        select ts,value_traffic_in,value_traffic_out from ods_snmp.ods_snmp_info_all where toDateTime(ts) >= '{config["begin_time"]}' and toDateTime(ts) <= '{config["end_time"]}' and tags_host_name = '{config["hostname"]}' and tags_ifName = '{config["interface"]}' order by ts asc
        '''
        dataset=self.conn.query(sql)
        dataset1=self.process(dataset[["ts","value_traffic_in"]].values.tolist())
        dataset2=self.process(dataset[["ts","value_traffic_out"]].values.tolist())
        x=fc(dataset1,config,"in",self.show,self.save)
        y=fc(dataset2,config,"out",self.show,self.save)
        self.count+=(x+y)

if __name__=="__main__":
    lt=[("SGSIN-SG3-J10003-C-02","xe-0/0/1:0")]
    config={
        "hostname":"",
        "interface":"",
        "begin_time":"2025-10-25 00:00:00",
        "end_time":"2025-11-06 00:00:00"
    }
    m=Test()
    for i in lt:
        try:
            config['hostname']=i[0]
            config['interface']=i[1]
            m.get(config)
        except Exception as e:
            print(config)
            print(e)
    print(m.count)