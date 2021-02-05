# Streaming Pipeline

## Prerequisites

Preffered use conda to create virtual env.
Install Anaconda if you don't have it and run command below.
It will create env.
```bash
conda env create -f environment.yaml
```

## Run locally from main
```bash
python main.py pipeline
```
## Kubernetis Setup

### Kafka
```bash
helm install my-release bitnami/kafka  --set externalAccess.enabled=true --set externalAccess.service.type=LoadBalancer --set externalAccess.service.port=9094 --set externalAccess.autoDiscovery.enabled=true --set serviceAccount.create=true --set rbac.create=true
```
### Cassandra
```bash
helm install cassandra bitnami/cassandra
```
