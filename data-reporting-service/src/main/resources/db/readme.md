# Liquibase exectuion

### Environment Variable set up

The following environment variables assume a local Database instance with ODSE and RDB databases
For any other instances, update the database url and credentials appropriately

```
export DB_USERNAME=sa
export DB_PASSWORD=fake.fake.fake.1234
export DB_URL=localhost 
export DB_ODSE=NBS_ODSE
export DB_RDB=RDB
```

### Liquibase script execution

* Make sure you are in the following project

    * `/NEDSS-DataAccess/data-reporting-service`

* Run following to execute liquibase

    * ```./gradlew update```
