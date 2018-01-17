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
	
```


### TODO 

- [x] 添加辅助测试工具，向mq中发送数据，观察消息再有消息处理的过程中，动态负载是否正确
- [ ] 架构图



        
### 测试
```shell


```






