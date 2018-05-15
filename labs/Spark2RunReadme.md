###Running Beam pipeline without spark-submit
The command work on spark2 cluster

####Running on Local cluster
```mvn exec:java  -Dexec.mainClass=org.gbif.pipelines.labs.spark2.WordCount -Dexec.args="--inputFile=/Users/clf358/gbif/pipelines/pom.xml --outputFile=finalresult"```

####Running on Standalone cluster
Need to copy the labs shaded jar manually first time to standalone spark cluster jars folder

```mvn exec:java  -Dexec.mainClass=org.gbif.pipelines.labs.spark2.WordCount -Dexec.args="--inputFile=/Users/clf358/gbif/pipelines/pom.xml --outputFile=finalresult --sparkMaster=spark://YN13797:7077"```

####Running on yarn cluster
```mvn exec:java  -Dexec.mainClass=org.gbif.pipelines.labs.spark2.WordCount -Dexec.args="--inputFile=/Users/clf358/gbif/pipelines/pom.xml --outputFile=finalresult --sparkMaster=yarn"```