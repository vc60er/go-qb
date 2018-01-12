# go-qb
Load balancer for rabbitmq queue subscribing

#### 安装rabbitmq
```shell
wget https://dl.bintray.com/rabbitmq/rpm/erlang/20/el/7/x86_64/erlang-20.1.7.1-1.el7.centos.x86_64.rpm
wget https://github.com/rabbitmq/rabbitmq-server/releases/download/v3.7.2/rabbitmq-server-3.7.2-1.el7.noarch.rpm.asc
wget https://github.com/rabbitmq/rabbitmq-server/releases/download/v3.7.2/rabbitmq-server-3.7.2-1.el7.noarch.rpm


rpm -i erlang-20.1.7.1-1.el7.centos.x86_64.rpm
yum install  rabbitmq-server-3.7.2-1.el7.noarch.rpm

rabbitmq-plugins enable rabbitmq_management

/etc/rabbitmq/rabbitmq.conf 
loopback_users.guest = false

http://59.110.154.248:15672
guest
guest

```

### 选型
```shell
embedding etcd
好处：
省的部署etcd服务
不存在与etcd直接的连接，所以不会担心这里出故障

坏处：
只有两个服务，且出现连接故障时候，难以选master


etcd:
好处
配置信息可以通过终端查看
配置服务与执行服务分离
```


### 架构
```shell
一个调度器：
	从多个执行器中挑选一个，持有调度器令牌锁才可以启动调度器
    从mq中拉取每个队列的状态，保存在queue_status_list中, 并定时更新器状态
    
	监新queue_status_list变化
	监视consumer_list变化
	在unsubscribed_queue_list或者consumer_list有变动时候触发一次负载分配，三秒内最多触发1次
 	负载分配：计算每个consumer应该订阅的队列数，并更新require_subscribe_queue_count

    负载逻辑:
        定时跟新并检查queue_status_list的变化, 如果有queue没有被订阅，则触发分配
        监视consumer_list变化, 增加减少时，触发分配
 	    负载：
            计算每个consumer应该订阅的队列数，并更新require_subscribe_queue_count


队列订阅申请
    从queue_list中找出，没有被订阅且没有被申请的队列返回
队列订阅失败
    
队列释放

队列状态始终同步于mq，可以被触发执行同步操作




多个执行器：
	启动后需要向调度器注册相关信息
	停止前需要向删除注册信息
	运行期间不断发送keepalive刷新存活状态
	重新负载触发条件
		consumer_list中require_subscribe_queue_count或者subscribed_queue_list发生变化
		周期性执行
	重新负载（订阅/释放）：
		if require_subscribe_queue_count > len(subscribed_queue_list)
			订阅
		else if require_subscribe_queue_count < len(subscribed_queue_list)
			释放
	订阅：
		申请队列
		if 失败:
			return
		订阅队列
		if 成功:
			添加consumer队列
		更新队列
				
	释放:
		选择consumer队列
		释放队列
		if 释放成功:
			更新队列
			删除consumer队列
		else:
			报告错误

	申请队列：
		for queue in queue_list:
			if queue.status == "" and queue.consumers==0:
				queue.status == "dispatched"
				return queue.name
	
	更新队列:
		queue.consumers = mq.Inspect().consumes
		if queue.consumers > 0:
			queue.status = "subscribed"
		else if queue.consumers == 0 && queue.status == "subscribed":
			queue.status = ""
			



	
数据结构：
	调度器令牌锁
	queue_list，consumer_list在etcd中一kv形式存储	未订阅队列list: queue_list，多点添加删除操作，需要安全锁
	消费者list: consumer_list，多点添加删除操作，需要安全锁



|key|value|
|---|---|
|consumer.$consumer_id.require_subscribe_queue_count|require_subscribe_queue_count|
|consumer.$consumer_id.queue_list|queue_list|
|queue.$queue_id|messages, consumers|


疑问：
	与etcd之间的连接断开后如何处理		

```


### TODO 
```shell
E0107 22:44:00.008526   26873 queue_balancer.go:246] executor_check_rebalance: err=Exception (530) Reason: "NOT_ALLOWED - attempt to reuse consumer tag 'queue.1.consumer'"
E0112 15:11:59.304502   22352 queue_balance.go:583] Exception (504) Reason: "channel/connection is not open"     

E0112 15:28:13.480721   27016 queue_balance.go:265] executor_subscribe: queue= err=no queue rest     




/qb/consumer_subscribed/consumer.1515743326
queue.7,queue.1,queue.8,queue.2
/qb/consumer_subscribed/consumer.1515743332
queue.3,queue.4,queue.5,queue.2
/qb/dispatcher
consumer.1515743326
/qb/lock/dispatcher/414660e8e8511ac0

/qb/lock/dispatcher/414660e8e8511ae0

/qb/queue/queue.1
{"Name":"queue.1","Messages":0,"Consumers":1}
/qb/queue/queue.2
{"Name":"queue.2","Messages":0,"Consumers":2}
/qb/queue/queue.3
{"Name":"queue.3","Messages":0,"Consumers":1}
/qb/queue/queue.4
{"Name":"queue.4","Messages":0,"Consumers":1}
/qb/queue/queue.5
{"Name":"queue.5","Messages":0,"Consumers":1}
/qb/queue/queue.6
{"Name":"queue.6","Messages":0,"Consumers":0}
/qb/queue/queue.7
{"Name":"queue.7","Messages":0,"Consumers":1}
/qb/queue/queue.8
{"Name":"queue.8","Messages":0,"Consumers":1}



108 E0112 16:19:32.531605   38280 queue_balance.go:233] executor_check_rebalance: err=Exception (530) Reason: "NOT_ALLOWED - attempt to reuse consumer tag 'queue.6.consumer'"
122 E0112 16:19:32.585822   38280 queue_balance.go:233] executor_check_rebalance: err=Exception (504) Reason: "channel/connection is not open"


失败时候没有解除保护

     82 I0112 16:58:52.027453   47171 queue_balance.go:263] executor_subscribe:
     83 I0112 16:58:52.027466   47171 queue_balance.go:536] queue_request:
     84 I0112 16:58:52.027486   47171 queue_balance.go:241] queue_load:
     85 I0112 16:58:52.027499   47171 queue_balance.go:397] get_kvs: prefix=/qb/queue/
     86 I0112 16:58:52.028396   47171 queue_balance.go:544] queue_request: qs={"Name":"queue.1","Messages":0,"Consumers":1}
     87 I0112 16:58:52.028406   47171 queue_balance.go:544] queue_request: qs={"Name":"queue.2","Messages":0,"Consumers":0}
     88 I0112 16:58:52.076673   47171 queue_balance.go:506] queue_protected_put_nx: key=/qb/queue_protected/queue.2 value=consumer.1515747531
     89 I0112 16:58:52.120850   47171 queue_mgr.go:48] Subscribe: queue=queue.2
     90 I0112 16:58:52.127071   47171 queue_balance.go:563] queue_status_changed_put: key=/qb/queue_status_changed/queue.2 value=consumer.1515747531.subscribed
     91 I0112 16:58:52.146598   47171 queue_balance.go:263] executor_subscribe:
     92 I0112 16:58:52.146619   47171 queue_balance.go:536] queue_request:
     93 I0112 16:58:52.146634   47171 queue_balance.go:241] queue_load:
     94 I0112 16:58:52.146648   47171 queue_balance.go:397] get_kvs: prefix=/qb/queue/
     95 I0112 16:58:52.147952   47171 queue_balance.go:544] queue_request: qs={"Name":"queue.1","Messages":0,"Consumers":1}
     96 I0112 16:58:52.147979   47171 queue_balance.go:544] queue_request: qs={"Name":"queue.2","Messages":0,"Consumers":0}
     97 I0112 16:58:52.187470   47171 queue_balance.go:506] queue_protected_put_nx: key=/qb/queue_protected/queue.2 value=consumer.1515747531
     98 I0112 16:58:52.235057   47171 queue_mgr.go:48] Subscribe: queue=queue.2
     99 E0112 16:58:52.241555   47171 queue_balance.go:273] executor_subscribe: queue=queue.2 err=Exception (530) Reason: "NOT_ALLOWED - attempt to reuse consumer tag 'queue.2.consumer'"
    100 E0112 16:58:52.241625   47171 queue_balance.go:233] executor_check_rebalance: err=Exception (530) Reason: "NOT_ALLOWED - attempt to reuse consumer tag 'queue.2.consumer'"


mq的相关操作失败后会断开连接

```

### 测试






