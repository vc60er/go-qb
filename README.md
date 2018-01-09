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
     
     
```

### 测试






