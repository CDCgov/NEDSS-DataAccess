#!/bin/sh
set -e

BASE="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

PATH=$BASE/nedss-dataaccess/builder/etl-data-pipeline
VERSION=master

# Clone NEDSSDev
rm -rf $PATH
cp -r $HOME/GitPrj/NEDSS-DataAccess/ $PATH
#git clone -b $VERSION git@github.com:cdcent/NEDSS-DataAccess.git $PATH

# Build and deploy database containers
echo "Building SQL Server database"
docker-compose -f $BASE/docker-compose.yml up nbs-mssql --build -d

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
rm -rf $PATH

echo "**** NEDSS DataAccess ETL Data Pipeline build complete ****"
echo "Health Check"
echo "http://localhost:8080/data-pipeline-status"
echo ""
echo "Database: localhost:1433"
echo "DB user: sa"
echo "DB password: fake.fake.fake.1234"
echo "Kafka: localhost:9092"
echo ""