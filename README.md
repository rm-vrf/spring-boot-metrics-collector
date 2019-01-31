# spring-boot-metrics-collector

`spring-boot-metrics-collector` 是一个 Spring Boot 指标收集器，可以从 `/metrics` 端口收集数据，存储到 Elasticsearch，实现指标统计。收集目标：

- Spring Boot 1.x 原生指标端口: `/metrics` ;
- Spring Boot 2.x Prometheus 指标端口: `/actuator/prometheus` ;
- 从 Eureka 发现端口，自动识别端口类型。

这里是一些资料，Spring Boot 指标端口和集成开发方式介绍：

- [https://github.com/lane-cn/spring-boot-metrics-sample](https://github.com/lane-cn/spring-boot-metrics-sample)
- [https://github.com/lane-cn/spring-boot-2-metrics-sample](https://github.com/lane-cn/spring-boot-2-metrics-sample)

`spring-boot-metrics-collector` 打包运行：

```shell
mvn package
```

创建配置文件 `metrics-collector.yaml`：

```yaml
############################# beat ######################################
beat:
  # Defines how often an event is sent to the output
  period: 10

  # Managment Adress
  hosts:
    - "http://localhost:28000/actuator/prometheus"
  
  # List of Metrics that shall be excluded from the collection
  excludes:
    - "system.load.average.1m"
    - "tomcat.global.error"

#-------------------------- Elasticsearch output -------------------------------
output.elasticsearch:

  # Array of hosts to connect to.
  # Scheme and port can be left out and will be set to the default (http and 9200)
  # In case you specify and additional path, the scheme is required: http://localhost:9200/path
  # IPv6 addresses should always be defined as: https://[2001:db8::1]:9200
  hosts: ["localhost:9200"]
  
  # Number of workers per Elasticsearch host.
  worker: 1
  
  # Optional index name. The default is "metrics-collector-" plus date
  # and generates [metrics-collector-]YYYY-MM-DD keys.
  # In case you modify this pattern you must update setup.template.name and setup.template.pattern accordingly.
  index: "'metrics-collector-'yyyy-MM-dd"
  
  # The maximum number of events to bulk in a single Elasticsearch bulk API index request.
  # The default is 50.
  bulk_max_size: 50
```

启动：

```
java -jar spring-boot-metrics-collector-1.0.0-SNAPSHOT.jar
```

可以使用 Eureka 自动发现服务位置，在启动命令中设置 `beat.eureka` 参数：

```shell
java -jar spring-boot-metrics-collector-1.0.0-SNAPSHOT.jar \
  --beat.eureka=http://localhost:8761/eureka/apps \
  --output.elasticsearch.hosts[0]=localhost:9200
```

Elasticsearch 集群地址也可以用启动参数指定：`output.elasticsearch.hosts`.
