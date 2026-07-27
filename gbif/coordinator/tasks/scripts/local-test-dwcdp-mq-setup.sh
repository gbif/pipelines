# create exchange
curl -u guest:guest -X PUT http://localhost:15672/api/exchanges/%2F/occurrence \
  -H "content-type: application/json" \
  -d '{"type":"topic","durable":true}'

# create queues
curl -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/pipelines-balancer \
  -H "content-type: application/json" \
  -d '{"durable":true}'

curl -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/dwcdp-nfs-to-hdfs-standalone \
  -H "content-type: application/json" \
  -d '{"durable":true}'

curl -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/dwcdp-nfs-to-hdfs-distributed \
  -H "content-type: application/json" \
  -d '{"durable":true}'

# bind queues
curl -u guest:guest -X POST http://localhost:15672/api/bindings/%2F/e/occurrence/q/pipelines-balancer \
  -H "content-type: application/json" \
  -d '{"routing_key":"pipelines.balancer"}'

curl -u guest:guest -X POST http://localhost:15672/api/bindings/%2F/e/occurrence/q/dwcdp-nfs-to-hdfs-standalone \
  -H "content-type: application/json" \
  -d '{"routing_key":"occurrence.dwcdp.nfs-to-hdfs.standalone"}'

curl -u guest:guest -X POST http://localhost:15672/api/bindings/%2F/e/occurrence/q/dwcdp-nfs-to-hdfs-distributed \
  -H "content-type: application/json" \
  -d '{"routing_key":"occurrence.dwcdp.nfs-to-hdfs.distributed"}'
