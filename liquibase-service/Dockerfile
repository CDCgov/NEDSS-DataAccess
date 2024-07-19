FROM liquibase/liquibase:latest-alpine

USER root

RUN apk add --no-cache wget unzip

RUN wget "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -O "awscliv2.zip" && \
  unzip awscliv2.zip && rm -rf awscliv2.zip && \
  ./aws/install


#Copy sources
COPY liquibase-service/src/main/resources/db/changelog /liquibase/changelog
COPY liquibase-service/src/main/resources/db/master /liquibase/changelog/src/main/resources/db/master/
COPY liquibase-service/src/main/resources/db/odse /liquibase/changelog/src/main/resources/db/odse
COPY liquibase-service/src/main/resources/db/rdb /liquibase/changelog/src/main/resources/db/rdb

#Set the Working Directory
WORKDIR /liquibase/changelog

USER liquibase

CMD ["sh", "-c", "tail -f /dev/null"]


