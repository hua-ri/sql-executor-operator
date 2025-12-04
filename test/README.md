# 测试数据库环境相关命令
启动所有数据库服务：
```shell
docker-compose up -d
```

测试完成后清理环境：
```shell
docker-compose down -v
```

重建完整环境：
```shell
docker-compose down -v && docker-compose up -d
```

创建sql任务：
```shell
kubectl apply -f sqljob-mysql-test.yaml
```

销毁sql任务：
```shell
kubectl delete -f sqljob-mysql-test.yaml
```

# 测试CRD相关命令
部署CRD资源
```shell
kubectl apply -f sqljob-mysql-test.yaml
```
销毁CRD资源
```shell
kubectl delete -f sqljob-mysql-test.yaml
```
