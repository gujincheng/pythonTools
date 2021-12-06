## 需要安装pip3 install python-kafka
## 本样例实现从某个时间点开始消费kafka数据
import time
import sys
import json

from kafka import KafkaConsumer



consumer = KafkaConsumer("gdm_fact_oms_orders_trade_detail",
                         bootstrap_servers=['10.251.10.54:9092','10.251.10.68:9092','10.251.10.69:9092'],
                         group_id="python_consumer_test_001",
                         consumer_timeout_ms=100000, max_poll_records=100,  # 每次最大消费数量
                         enable_auto_commit=True,  # 每过一段时间自动提交所有已消费的消息（在迭代时提交）
                         auto_commit_interval_ms=5000)
consumer.poll(timeout_ms=10000, max_records=100, update_offsets=True)
assignment = consumer.assignment()
timeStr = "2021-12-06 15:51:00"
formatTime = time.strptime(timeStr, '%Y-%m-%d %H:%M:%S')
startTime = int(time.mktime(formatTime)) * 1000

timestampToSearch = {}
for tp in assignment:
    timestampToSearch[tp] = startTime
offsets = consumer.offsets_for_times(timestampToSearch)

for tp in assignment:
    offsetAndTimestamp = offsets[tp]
    if offsetAndTimestamp is not None:
        consumer.seek(tp, offsetAndTimestamp.offset)

if __name__ == '__main__':
    for msg in consumer:
        value = json.loads(msg.value)
        value.pop('etl_date')
        value.pop('opt_time')
        if index < 10:
        if json.dumps(value) in st :
           pt = str(msg.partition)
           offset = str(msg.offset)
           ftrade_id = str(value['ftrade_id'])
           aaa = str(json.loads(msg.value))
           print('####'.join([pt,offset,ftrade_id,str(value)]))
