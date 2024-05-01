# Liquibase exectuion

### Environment Variable set up
From terminal execute following in running a local Database instance:

```
export DB_USERNAME=sa
export DB_PASSWORD=fake.fake.fake.1234
export DB_URL=localhost 
export DB_ODSE=NBS_ODSE
export DB_RDB=RDB
```

Note: If you are running against a different instance, 
replace the values for Database credentials, url, odse and rdb database as appropriate

### Liquibase script execution

* Make sure you are in the following project

  * `/NEDSS-DataAccess/data-reporting-service`

* Run following to execute liquibase

  *   ```./gradlew update```
