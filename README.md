# Flink example 

This example is example of how to setup flink 11, jdk 11, springboot, k8s cluster, aliyun

## Flink 11, jdk 11
```bash
# build flink11 jdk11 image
docker build -t flink11-jdk11 jdk11-docker
```

## Flink Aliyun k8s deployment
Replace image repo in `deploy/job-manager.yaml` and `anddeploy/task-manager.yaml` file 
Replace oss setup in yaml `storage.yaml` file

```bash
kubectl apply -f deploy/
```

This will deploy 1 job-manager, 3 task-manager, and setup the oss to save checkpoints

## Flink springboot
Flink can't work with `gradle bootJar`, so we should generate Jar with `gradle clean shadowJar`
All settings are in build.gradle
 