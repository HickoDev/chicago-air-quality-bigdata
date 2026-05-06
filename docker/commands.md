# Docker Commands

The project follows the professor's TP environment based on `liliasfaxi/hadoop-cluster:latest`.

## Pull the image

```powershell
docker pull liliasfaxi/hadoop-cluster:latest
```

## Create the Docker network

```powershell
docker network create --driver=bridge hadoop
```

## Start the three-container cluster

```powershell
docker run -itd --net=hadoop -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 --name hadoop-master --hostname hadoop-master liliasfaxi/hadoop-cluster:latest

docker run -itd -p 8040:8042 --net=hadoop --name hadoop-worker1 --hostname hadoop-worker1 liliasfaxi/hadoop-cluster:latest

docker run -itd -p 8041:8042 --net=hadoop --name hadoop-worker2 --hostname hadoop-worker2 liliasfaxi/hadoop-cluster:latest
```

## Re-start existing containers

```powershell
docker start hadoop-master hadoop-worker1 hadoop-worker2
```

## Enter the master container

```powershell
docker exec -it hadoop-master bash
```

## Start Hadoop services

```bash
./start-hadoop.sh
```

## Useful checks

```bash
jps
hdfs dfsadmin -report
```

## Useful UI links

- Hadoop NameNode: http://localhost:9870
- YARN ResourceManager: http://localhost:8088
- HBase UI: http://localhost:16010
