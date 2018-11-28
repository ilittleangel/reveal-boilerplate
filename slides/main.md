# monitor Spark Streaming
Obteniendo métricas de un proceso Spark Streaming



### Quien soy
- Angel Rojo
- Data Engineer en Orange
- Tribu de datos
- Squad de Arquitectura

![Datafreak](images/DataFreak_fondonaranja.jpg)



### El problema
- Las aplicaciones streaming corren 24x7
- Métodos de monitorización no invasivos
- Alarmado automático



### Solución
Hemos desarrollado una aplicación Python que:
- consulta que procesos monitorizar y su modo de ejecución
- realiza la toma de datos y extracción de métricas
- almacena en Elasticsearch / SolR
- se despliega en Kubernetes
- alarmado con Elastalert
- dashboards Grafana/kibana



### Solución

![Diagrama flujo datos](images/spark_monitor_diagram_crop.jpeg)



### Qué monitorizamos
```
spark-submit \
  --master yarn \
  --deploy-mode cluster \
```
```
  --num-executors ${EXECUTORS} \
  --executor-cores ${EXECUTOR_CORES} \
  --driver-memory ${DRIVER_MEMORY} \
  --executor-memory ${EXECUTOR_MEMORY} \
  --conf spark.dynamicAllocation.enabled=false \
  ...
  --keytab ${KEYTAB} \
  --principal ${PRINCIPAL} \
  --class ${DRIVER_CLASS} \
  ${JAR} ${@}
```



### `spark-monitor`

- Aplicación **Python3.6**
- Consultas al API de **YARN**:
  - Resource Manager
  - Application Master - Driver Spark
- Recopilación de métricas
  - métricas en modo client -> stages de Spark
  - métricas en modo cluster -> Log del driver que corre en el AM
- Indexado en **Elasticsearch**
- Actualización de la **bbdd**



### Un ciclo de ejecución

```log
[DEBUG]: Parsing configuration file: /spark-monitor/config.ini
[INFO]: Deployment environment in k8s: $MY_NODE_NAME=None
[INFO]: monitoring `PRO` environment
[DEBUG]: Looking for active RM: http://production_machine01.1:8088/ws/v1/cluster/info
[DEBUG]: Looking for active RM: status=`ACTIVE`
[DEBUG]: Connected to `ELASTICSEARCH` in `PRO` environment
[DEBUG]: Health of `ELASTICSEARCH` cluster: status=`GREEN`
[DEBUG]: Connected to `./db/monitor.db`
[DEBUG]: url: http://production_machine01.1:8088/ws/v1/cluster/apps?states=running&applicationTypes=spark
[DEBUG]: apps to monitor: ['application1Cep', 'orange.bigdata.application1.Boot']
[DEBUG]: apps with RUNNING state: ['orange.bigdata.application1.Boot']
[DEBUG]: monitoring app: `orange.bigdata.application1.Boot`
[DEBUG]: {
    "id": "application_01",
    "user": "orangede",
    "name": "orange.bigdata.application1.Boot",
    "queue": "root.users.orangede",
    "state": "RUNNING",
    "finalStatus": "UNDEFINED",
    "progress": 10.0,
    "trackingUI": "ApplicationMaster",
    "trackingUrl": "http://worker3:8088/proxy/application_01/",
    "diagnostics": "",
    "clusterId": 1542620249595,
    "applicationType": "SPARK",
    "applicationTags": "",
    "startedTime": 1543312817735,
    "finishedTime": 0,
    "elapsedTime": 23677260,
    "amContainerLogs": "http://worker1:8042/node/containerlogs/container_01_01_000001/orangede",
    "amHostHttpAddress": "worker1:8042",
    "allocatedMB": 379904,
    "allocatedVCores": 41,
    "reservedMB": 0,
    "reservedVCores": 0,
    "runningContainers": 41,
    "memorySeconds": 8991373888,
    "vcoreSeconds": 970357,
    "preemptedResourceMB": 0,
    "preemptedResourceVCores": 0,
    "numNonAMContainerPreempted": 0,
    "numAMContainerPreempted": 0,
    "logAggregationStatus": "NOT_START"
}
[DEBUG]: Spark API: url: http://worker3:8088/proxy/application_01/api/v1/applications/application_01/environment
[DEBUG]: Spark API: ['spark.submit.deployMode', 'cluster']
[DEBUG]: SQLite: Recovering the last bytes read
[DEBUG]: SQLite: bytes=`268500891`
[DEBUG]: http://worker1:8042/node/containerlogs/container_01_01_000001/orangede/stderr/?start=268500891
[DEBUG]: elasticsearch: Indexing app log
[DEBUG]: elasticsearch: Indexed `13` elements
[DEBUG]: elasticsearch: Process `31609883` bytes
[DEBUG]: SQLite: Update (app_id=`application_01`, bytes=`300110774`)
```



### spark-monitor -  `config.ini`

```
[DEBUG]: Parsing configuration file: /spark-monitor/config.ini
```

```python
[ENVS]
yarn = pro
elastic = pro

[APPS]
names = [
        "application1Cep",
        "orange.bigdata.application1.Boot"
        ]
window = 30

[LOGGING]
level = INFO

[ERRORS]
max_sleep_time = 10
min_sleep_time = 5
max_errors_threshold = 10
min_errors_threshold = 5
```



### spark-monitor -  `settings` 1/2

```python
config = configparser.ConfigParser()
config_file = f'{ROOT_DIR}/config.ini'
config.read(config_file)
```

```python
YARN_ENV = config['ENVS']['yarn'].lower()
INDEX_ENV = config['ENVS']['elastic'].lower()
names = json.loads(config['APPS']['names'])
errors = config['ERRORS']
MAX_SLEEP_TIME = errors.getint('max_sleep_time', fallback=10)
MIN_SLEEP_TIME = errors.getint('min_sleep_time', fallback=5)
MAX_ERRORS_THRESHOLD = errors.getint('max_errors_threshold', fallback=10)
MIN_ERRORS_THRESHOLD = errors.getint('min_errors_threshold', fallback=5)
```



### spark-monitor - `settings` 2/2

```python
ENVS = {
    "pro": {
        "rm": ["http://production_machine01:8088/ws/v1/cluster",
               "http://production_machine02:8088/ws/v1/cluster"],
        "elasticsearch": {
            "01": ["http://elastic-01.aot:9200"],
            "02": ["http://elastic-02.aot:9200"]
        }
    },
    "pre": {
        "rm": ["http://preproduction_machine01:8088/ws/v1/cluster"],
        "elasticsearch": ["http://elastic-pre.pre:9200"]
    }
}
```



### spark-monitor - `pipeline` 1/4

```log
[INFO]: monitoring `PRO` environment
[DEBUG]: Looking for active RM: http://production_machine01.1:8088/ws/v1/cluster/info
[DEBUG]: Looking for active RM: status=`ACTIVE`
[DEBUG]: Connected to `ELASTICSEARCH` in `PRO` environment
[DEBUG]: Health of `ELASTICSEARCH` cluster: status=`GREEN`
[DEBUG]: Connected to `./db/monitor.db`
```

```python
rm = yarn.get_active_rm(ENVS[YARN_ENV]['rm'])
es = elastic.create_connection()
db = sqlite.monitorDb(db_conn)
```



### spark-monitor - `pipeline` 2/4

Determinamos el modo de ejecución

```python
def get_active_rm(rms):
    for rm in rms:
        try:
            url = f"{rm}/info"
            request = requests.get(url, proxies=NO_PROXY)
            request.raise_for_status()
            status = request.json()['clusterInfo']['haState']
            if status == 'ACTIVE':
                return rm
        except requests.exceptions.RequestException as re:
            pass
    else:
        pass
```



### spark-monitor - `pipeline` 3/4

```log
[DEBUG]: url: http://production_machine01.1:8088/ws/v1/cluster/apps?states=running&applicationTypes=spark
[DEBUG]: apps to monitor: ['application1', 'orange.bigdata.application1.Boot']
[DEBUG]: apps with RUNNING state: ['orange.bigdata.application1.Boot']
```

```python
apps = [(name, app)
    for name in names
    for app in yarn.request_running_apps(rm)
    if app['name'] == name]

for app in apps:
    res = process_app(es, db, app)
    ...
```



### spark-monitor - `pipeline` 4/4

```log
[DEBUG]: Spark API: url: http://worker3:8088/proxy/application_01x/api/v1/applications/application_01/environment
[DEBUG]: Spark API: ['spark.submit.deployMode', 'cluster']
```

```python
def process_app(es, db, app):
    mode = yarn.get_deploy_mode(app)

    if mode == "cluster":
        return cluster.monitoring(es, app, db), app['name']
    elif mode == "client":
        return client.monitoring(es, app, db), app['name']
    else:
        pass
```



### spark-monitor - Métricas - Submit Cluster

```log
[DEBUG]: SQLite: Recovering the last bytes read
[DEBUG]: SQLite: bytes=`268500891`
[DEBUG]: http://worker1:8042/node/containerlogs/container_01_01_000001/orangede/stderr/?start=268500891
[DEBUG]: elasticsearch: Indexing app log
[DEBUG]: elasticsearch: Indexed `13` elements
[DEBUG]: elasticsearch: Process `31609883` bytes
[DEBUG]: SQLite: Update (app_id=`application_01`, bytes=`300110774`)
```

```python
def app_url(app, start):
    return f"{app['amContainerLogs']}/stderr/?start={start}"

def monitoring(es, app, db, db_bytes):
    url = app_url(app, start=db_bytes)
    applog, size = get_applog(url)
    total = size + int(db_bytes)
    es.index(applog)
    db.upsert(app, total, 'cluster')
```



### spark-monitor - Métricas - Submit Client 1/2

```python
def app_url(app, url_type, stageid="", jobid=""):
    endpoint = f"{app['trackingUrl']}api/v1/applications/{app['id']}"
    url_types = {"stages": f"{endpoint}/stages/{stageid}",
                 "jobs":   f"{endpoint}/jobs/{jobid}"}
    return url_types.get(url_type)
```



### spark-monitor - Métricas - Submit Client 2/2

```python
def monitoring(es, app, db):
    url_job = set_url(app, url_type='jobs')
    jobs = requests.get(url_job, proxies=NO_PROXY).json()
    jobids = [x['jobId'] for x in jobs if x['jobId'] > db.get('jobid')]
    jobids.reverse()

    for jobid in jobids:
        url_job = set_url(app, url_type='jobs', jobid=jobid)
        response_job = requests.get(url_job, proxies=NO_PROXY)
        job = response_job.json()
        
        for stageid in job['stageIds']:
            url_stage = set_url(app, url_type='stages', stageid=stageid)
            stage = requests.get(url_stage, proxies=NO_PROXY).json()
            status = stage[0]['status']

            if status == 'COMPLETE':
                new_stage = join(list(map(partial(to_job_and_stages), [job])), stage)
                es.index(new_stage, app)
                db.update(app, stageid, datetime.now().isoformat(), job['jobId'])
            else:
                pass
```




### Ejemplo Métricas

Documento indexado para la aplicación Spark Structured Streaming **`orange.bigdata.application1.Boot`**

```json
{
  "_index": "monitor-yarn-pro-20181128",
  "_type": "metrics",
  "_id": "SvOTWmcBSwlc5ws799ZZ",
  "_version": 1,
  "_score": null,
  "_source": {
    "@timestamp": "2018-11-28T13:48:06.268000",
    "appName": "orange.bigdata.application1.Boot",
    "appId": "application_1537441422662_18819",
    "deployMode": "cluster",
    "running": true,
    "metrics": {
      "id": "d64a41c2-ed97-4824-b67f-02c5371c4e3f",
      "runId": "089182cf-54e6-4448-9498-a4dac0208b2f",
      "name": null,
      "timestamp": "2018-11-28T13:48:06.268Z",
      "batchId": 956557,
      "numInputRows": 644286,
      "inputRowsPerSecond": 59777.8808684357,
      "processedRowsPerSecond": 67134.10440762738,
      "durationMs": {
        "addBatch": 6558,
        "getBatch": 6,
        "getOffset": 2748,
        "queryPlanning": 73,
        "triggerExecution": 9597,
        "walCommit": 210
      },
      "stateOpera01s": [
        {
          "numRowsTotal": 2635230,
          "numRowsUpdated": 19351,
          "memoryUsedBytes": 924016415
        }
      ],
      "sources": [
        {
          "description": "KafkaSource[Subscribe[topic1]]",
          "endOffset": {
            "topic1": {
              "0": 2525444964,
              "1": 2464258770,
              "2": 2470201063                            
            }
          }
        }
      ]
    }
  }
}
```



### Dashboard

[link to Dashboard](https://spark-monitor.kibana.es/goto/b232cdcd3164e16e1a5cb270705579de)



### Deploy 1/2

- Se despliega en Kubernetes
- Empaquetamos el codigo en una imagen Docker

```
FROM python:3.6.5-slim-stretch
LABEL project="monitor"
MAINTAINER Angel Rojo <>

RUN mkdir /spark-monitor
COPY monitor/requirements.txt spark-monitor

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends g++ make \
    && mkdir /spark-monitor/logs /spark-monitor/db \
    && pip3.6 install -r /spark-monitor/requirements.txt
```



### Deploy 2/2

```
COPY monitor/utils            spark-monitor/utils
COPY monitor/yarn             spark-monitor/yarn
COPY monitor/monitor.py       spark-monitor
COPY monitor/settings.py      spark-monitor

WORKDIR spark-monitor

ENTRYPOINT ["python", "/spark-monitor/monitor.py"]
```



### Kubernetes

Componentes desplegados

  - spark-monitor
  - elasticsearch
  - kibana
  - spark-alarmer (Elastalert)



#### Kubernetes - `spark-monitor-deployment.yaml`

En este fichero definimos todas las propiedades de nuestro Deployment.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-monitor
  namespace: aot
  labels:
    k8s-app: spark-monitor
    version: v1.7
    deploy-version: v201811191255
    project: gea-monitorizacion
spec:
  replicas: 1
  selec01:
    matchLabels:
      k8s-app: spark-monitor
      project: gea-monitorizacion
  template:
    metadata:
      labels:
        k8s-app: spark-monitor
        version: v1.7
        deploy-version: v201811191255
        project: gea-monitorizacion
    spec:
      containers:
      - image: registry:3333/spark-monitor:1.7-pro
        name: spark-monitor
        env:
        - name: MY_NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        imagePullPolicy: Always
        volumeMounts:
          - name: config
            mountPath: /spark-monitor/config.ini
            subPath: config.ini
      restartPolicy: Always
      volumes:
      - name: config
        configMap:
          name: spark-monitor
          items:
          - key: monitor_config
            path: config.ini

```



### Kubernetes - `spark-monitor-cm.yaml`

Creamos el fichero `config.ini` 

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: spark-monitor
  namespace: aot
  labels:
    project: gea-monitorizacion
    deploy-version: v201811191255
data:
  monitor_config: |
    [ENVS]
    yarn = pro
    elastic = pro
    
    [APPS]
    names = ["orange.bigdata.application1.Boot", "application1Boot"]
  ...
```



### Kubernetes - `spark-alarmer-cm.yaml`

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{service}}
  namespace: {{k8sNamespace}}
  labels:
    project: gea-monitorizacion
    deploy-version: {{tag}}
data:
  elastalert_config: |-
    rules_folder: /opt/rules
    scan_subdirectories: false
    run_every:
      minutes: 1
    buffer_time:
      minutes: 15
    es_host: {{es_host}}
    es_port: 9200
    writeback_index: elastalert_status_{{service}}
    alert_time_limit:
      days: 2

  elastalert_rule_application1_not_running: |-
    es_host: {{es_host}}
    es_port: 9200
    name: Proceso Ups is NOT Running
    type: frequency
    index: monitor-yarn-*
    num_events: 2
    timeframe:
        minutes: 5
    realert:
        minutes: {{realert_minutes}}
    filter:
    - query:
        query_string:
          query: "running: false AND appName: orange.bigdata.application1.Boot"
    alert:
    - "command"
    command: ["echo", "{{service}}|P2|GEA|{{k8sNamespace}}|ERROR CRITICAL: %(appName)s is NOT running"]
    pipe_alert_text: true
    new_style_string_format: false
```



![Kubernetes](images/kubernetes-console.jpeg)



### Fin

Preguntas?



![Kubernetes](images/we_are_hiring.jpg)

