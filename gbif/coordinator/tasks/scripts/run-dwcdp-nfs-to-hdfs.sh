java --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
  -jar ../target/tasks-coordinator-cli-4.0.20-SNAPSHOT-shaded.jar pipelines-balancer \
  --log-config ./local-test-logback.xml \
  --conf ./local-test-config-dwcdp-nfs-to-hdfs.yaml
