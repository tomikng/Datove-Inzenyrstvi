# Homework 2 - Apache Airflow
Hai Hung Nguyen

### Struktura
- Všechny releavntní soubory jsou umístěné v adresáři `airflow`
- Obsahuje `Dockerfile` a `docker-compose` složky spolu s `requirements.txt`
- `data_cube_dag.py` DAG je obsažen v adresáři DAG, spolu s trošku upravenéma skripty z předchozího domácího úkolu
### Systemové požadavky
- Docker
- Docker Compose

### Quick start
Nejdříve se dostat do správného adresáře
```commandline
cd airflow
```
Poté spustíme docker
```commandline
docker build . --tag extending_airflow:latest
```
Spustíme web server a scheduler pomocí `docker-compose`
```commandline
docker-compose up -d
```
Zkontrolujeme lokální Airflow WebServer dostupný na adrese http://0.0.0.0:8080/dags/data-cube-dag/ a spustíme DAG
s konfigurací (Trigger DAG w/ config) obsahující JSON parametr pro proměnnou "output_path" nastavený na absolutní cestu 
v Linuxovém souborovém systému (např. {"output_path": "/opt/airflow/dags/"}).

Spuštěním `docker compose down --volumes --rmi all` zastavíme a smažeme věechny kontajnery
### 

