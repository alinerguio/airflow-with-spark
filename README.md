## Running local environment

As MWAA service can differ from the local environment, this is advised only for testing quicker.

In no hypothesis, local test substitutes the staging workflow.
> [!NOTE] 
> The best and quickest test is just running the dag with python in your terminal before the following tests.

1. Install [Docker](https://www.docker.com/products/docker-desktop/)
2. Run the following command line in the local/docker folder
```sh
docker compose up
```
3. Log in with airflow as user and password in [localhost:8080](http://localhost:8080/)
4. Open [localhost:8888](http://localhost:8888/) to use notebooks and [localhost:8181](http://localhost:8181/) to check the spark application
5. Put your dags in the dags folder (created in the airflow directory)
6. If requirements are necessary, add them to the line 74 in the docker-compose.yml file and don't forget to add it to the **requirements.txt** file afterwards (in docker/jupyter for local dev and in the root folder to be added in deployed airflow)
```
_PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-deltalake pandas}
```
7. If Airflow in AWS image updated, update this one as well in line 53.
8. To run spark files outside of a DAG, use the following command:
```sh
docker exec -it docker-spark-worker-1-1 spark-submit --master spark://spark:7077 /usr/local/spark/app/hello-world.py /usr/local/spark/resources/data/airflow.cfg
```

## Developing locally

It also may be useful to use the notebooks folder to create the base code for your pipelines, as Jupyter Notebook is great for debugging. A Jupyter Notebook will be deployed when running docker compose in port [8888](http://localhost:8888/). 

## Dockerfile in docker folder
### Airflow
The Dockerfile is based in this [airflow image](https://airflow.apache.org/docs/docker-stack/build.html#example-of-adding-airflow-provider-package-and-apt-package) with a specific python version to match spark's python version.

To build this image and push it, it is necessary to run the following command lines inside local/docker/airflow:
```sh
docker build . --tag arodriguesblg/airflow-spark:2.7.3
```
```sh
docker push arodriguesblg/airflow-spark:2.7.3 
```
### Jupyter Notebook
This Dockerfile encompasses the requirements needed to create the dags.
To build this image and push it, it is necessary to run the following command lines inside local/docker/jupyter:
```sh
docker build . --tag arodriguesblg/pyspark-notebook:spark-3.5.0
```
```sh
docker push arodriguesblg/pyspark-notebook:spark-3.5.0
```