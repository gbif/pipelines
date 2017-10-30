# Setup SOLR 5 server for test
_These are just intended for private notes during the proof of concept_

To set up the SOLR 5 server on the dev cluster you do something like so:  

### copy the config to the gateway
```
scp -r solr trobertson@c3gateway-vh.gbif-dev.org:/home/trobertson
ssh trobertson@devgateway-vh.gbif.org
```

### install the configs into ZK 
```
/opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/lib/solr/bin/zkcli.sh -zkhost c3master1-vh.gbif.org:2181,c3master2-vh.gbif.org:2181,c3master3-vh.gbif.org:2181/solr5c2 -cmd upconfig -confname beam-demo1 -confdir ./solr/conf
```

### create a collection
```
solrctl --solr http://c3n1.gbif.org:8983/solr --zk c3master1-vh.gbif.org:2181,c3master2-vh.gbif.org:2181,c3master3-vh.gbif.org:2181,c1n3.gbif.org:2181/solr5c2 collection --create beam-demo1 -s 3 -c beam-demo1 -r 1 -m 3
```

### recreate the collection
```
solrctl --solr http://c3n1.gbif.org:8983/solr --zk c3master1-vh.gbif.org:2181,c3master2-vh.gbif.org:2181,c3master3-vh.gbif.org:2181,c1n3.gbif.org:2181/solr5c2 collection --delete beam-demo1

solrctl --solr http://c3n1.gbif.org:8983/solr --zk c3master1-vh.gbif.org:2181,c3master2-vh.gbif.org:2181,c3master3-vh.gbif.org:2181,c1n3.gbif.org:2181/solr5c2 collection --create beam-demo1 -s 3 -c beam-demo1 -r 1 -m 3
```

http://c3n1.gbif.org:8983/solr/beam-demo1/select?q=*%3A*&wt=json&indent=true
