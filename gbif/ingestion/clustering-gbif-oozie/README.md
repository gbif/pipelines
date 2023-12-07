# Oozie workflow

Install by building the shaded artifact and using the script:

```
cd ../clustering-gbif
mvn install -Pextra-artifacts

cd ../clustering-gbif-oozie
./install-workflow.sh dev GITHUB_KEY
```

