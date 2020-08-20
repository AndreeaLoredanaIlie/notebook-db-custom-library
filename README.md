# notebook-db-custom-library

## Add a custom library to a Databricks Scala notebook

This class incorporates some common functions such us: connection to DB, read, write, update table
that can be used in every notebook.

To read a table with SELECT query
```bash
readDBWithQuery
```

To read a full table without any query
```bash
readDB
```

To write the table to database
```bash
writeDB
```

To update the conformed layer target table using primary key and list of primary keys in database and delta lake
```bash
updateTargetTable
```
