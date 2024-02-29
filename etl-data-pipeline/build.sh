#!/bin/sh
set -e

BASE="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

#DA_PATH=$BASE/nedss-dataaccess/builder/ #Uncomment for copying from github repo

# Clone NEDSSDev
#rm -rf $DA_PATH
#cp -r $HOME/'GitPrj/NEDSS-DataAccess/etl-data-pipeline/' $DA_PATH

#VERSION=master
#git clone -b $VERSION git@github.com:cdcent/NEDSS-DataAccess.git $DA_PATH

# Build and deploy database containers
echo "Building SQL Server database"
docker-compose -f $BASE/docker-compose.yml up nbs-dataaccess-mssql --build -d

# Build and deploy Zookeeper container
echo "Building Zookeeper"
docker-compose -f $BASE/docker-compose.yml up zookeeper --build -d

# Build and deploy Kafka Broke container
echo "Building Kafka Broker"
docker-compose -f $BASE/docker-compose.yml up broker --build -d

# Build and deploy ETL Data pipeline container
echo "Building ETL Data Pipeline"
docker-compose -f $BASE/docker-compose.yml up etl-data-pipeline --build -d

# Cleanup 
#rm -rf $DA_PATH

echo "**** NEDSS DataAccess ETL Data Pipeline build complete ****"
echo "Health Check"
echo "http://localhost:8080/data-pipeline-status"
echo ""
echo "Database: localhost:1433"
echo "DB user: sa"
echo "DB password: fake.fake.fake.1234"
echo "Kafka: localhost:9092"
echo ""