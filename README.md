# go-qb
Load balancer for rabbitmq queue subscribing



## Feature
- Rabbitmq queue subscription load balancing based on etcd




## Installation
install:

	go get -u github.com/vc60er/go-qb
	
import:

	import "github.com/vc60er/go-qb"


## Quickstart
```go
type MyQueueOnMsg struct {
}

func (pthis *MyQueueOnMsg) OnMsg(msg []byte) {
	log.Info("OnMsg:", string(msg))
}

func main() {
	flag.Parse()

	msg := &MyQueueOnMsg{}
	endpoints := []string{"http://127.0.0.1:2379", "http://127.0.0.1:22379", "http://127.0.0.1:32379"}
	amqp := "amqp://guest:guest@59.110.154.248:5672/"
	queue_ids := []string{"queue.1", "queue.2", "queue.3", "queue.4", "queue.5", "queue.6", "queue.7", "queue.8"}
	ip := "127.0.0.1"
	port := 8800

	qb, err := qb.NewQueueBalance(endpoints, amqp, msg, queue_ids, ip, port)
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}

	defer qb.Close()

	qb.Run()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	s := <-c
	log.Info(s)
}
```





### Architecture diagram
![](https://user-images.githubusercontent.com/18476122/35110944-fc6307f8-fcb4-11e7-95a6-220d809c59e5.png)


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


        
### 测试



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

```shell

cd example

运行etcd
goreman start

运行consuer1
make run1

运行consuer2
make run2

运行consuer3
make run2

向mq发布消息
make pub

观察运行状态
make watch

```

```bash
Every 1.0s: ./cli.sh get --prefix /qb                                                                                                                     chengningMBP.lan: Thu Jan 18 20:48:05 2018

/qb/consumer_require_queue_count/consumer_127.0.0.1_3632541266441280445
3
/qb/consumer_require_queue_count/consumer_127.0.0.1_3632541266441280505
3
/qb/consumer_require_queue_count/consumer_127.0.0.1_4703553552825333576
2
/qb/consumer_status/consumer_127.0.0.1_3632541266441280445
queue.7,queue.8,queue.4
/qb/consumer_status/consumer_127.0.0.1_3632541266441280505
queue.2,queue.5,queue.6
/qb/consumer_status/consumer_127.0.0.1_4703553552825333576
queue.1,queue.3
/qb/keeper_api
http://127.0.0.1:8800
/qb/lock/keeper/326961092a2e1bbd

/qb/lock/keeper/326961092a2e1bf9

/qb/lock/keeper/414661092a2e3748

/qb/queue_status/queue.1
{"Name":"queue.1","Messages":0,"Consumers":1}
/qb/queue_status/queue.2
{"Name":"queue.2","Messages":0,"Consumers":1}
/qb/queue_status/queue.3
{"Name":"queue.3","Messages":0,"Consumers":1}
/qb/queue_status/queue.4
{"Name":"queue.4","Messages":0,"Consumers":1}
/qb/queue_status/queue.5
{"Name":"queue.5","Messages":0,"Consumers":1}
/qb/queue_status/queue.6
{"Name":"queue.6","Messages":0,"Consumers":1}
/qb/queue_status/queue.7
{"Name":"queue.7","Messages":0,"Consumers":1}
/qb/queue_status/queue.8
{"Name":"queue.8","Messages":0,"Consumers":1}
```





### TODO 

- [x] 添加辅助测试工具，向mq中发送数据，观察消息再有消息处理的过程中，动态负载是否正确
- [x] 架构图






