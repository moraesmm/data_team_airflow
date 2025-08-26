# data_team_airflow

## Obj do projeto
Relembrar os detalhes de funcionamento do Airflow.

## Inicializando infra
Referencia - [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/2.0.2/start/docker.html#docker-compose-yaml)

``` cmd
 docker-compose up airflow-init
 docker-compose up
```

## Webserver
[localhost](http://localhost:8080)



#### Docker Compose details
This file contains several service definitions:

- airflow-scheduler - The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.

- airflow-webserver - The webserver available at http://localhost:8080.

- airflow-worker - The worker that executes the tasks given by the scheduler.

- airflow-init - The initialization service.

- flower - The flower app for monitoring the environment. It is available at http://localhost:8080.

- postgres - The database.

- redis - The redis - broker that forwards messages from scheduler to worker.

All these services allow you to run Airflow with CeleryExecutor. For more information, see Basic Airflow architecture.

Some directories in the container are mounted, which means that their contents are synchronized between your computer and the container.

- ./dags - you can put your DAG files here.

- ./logs - contains logs from task execution and scheduler.

- ./plugins - you can put your custom plugins here.