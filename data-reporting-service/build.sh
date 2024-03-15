#!/bin/sh
set -e

BASE="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

#DA_PATH=$BASE/nedss-dataaccess/builder/ #Uncomment for copying from github repo

# Clone NEDSSDev
#rm -rf $DA_PATH
#cp -r $HOME/'GitPrj/NEDSS-DataAccess/data-reporting-service/' $DA_PATH

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
echo "Updating the th Data Pipeline"
docker-compose -f $BASE/docker-compose.yml up data-reporting-service --build -d

echo "Updating the th Person Reporting Service"
docker-compose -f $BASE/docker-compose.yml up person-reporting-service --build -d

# Cleanup 
#rm -rf $DA_PATH

echo "**** NEDSS DataAccess ETL Data Pipeline build complete ****"
echo "Reporting Service Health Check"
echo "http://localhost:8081/status"
echo ""
echo "Person Reporting Service Health Check"
echo "http://localhost:8090/status"
echo ""
echo "Database: localhost:1433"
echo "DB user: sa"
echo "DB password: fake.fake.fake.1234"
echo "Kafka: localhost:9092"
echo ""