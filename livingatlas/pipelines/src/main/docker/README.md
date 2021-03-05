# Docker for development

These docker compose files can use for local development.

If you are running integration tests, 
please use the following to run
containers to work with the tests:

```
mvn docker:start
```

and to stop:

```
mvn docker:stop
```

These commands will start containers for **SOLR**, **ala-namematching-service**
and **ala-sensitive-data-service**.