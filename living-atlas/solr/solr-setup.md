# Setup SOLR 5 server for test
_These are just intended for private notes during the proof of concept_

To set up the SOLR 5 server on the dev cluster you do something like so:  

### copy the config to the gateway
scp -r solr trobertson@devgateway-vh.gbif.org:/home/trobertson
ssh trobertson@devgateway-vh.gbif.org

### install the configs into ZK 
/opt/cloudera/parcels/CDH-5.12.0-1.cdh5.12.0.p0.29/lib/solr/bin/zkcli.sh -zkhost c1n1.gbif.org:2181,c1n2.gbif.org:2181,c1n3.gbif.org:2181/solr5dev -cmd upconfig -confname tim-untyped-test -confdir ./solr/conf

### create a collection
solrctl --solr http://c2n1.gbif.org:8983/solr --zk c1n1.gbif.org:2181,c1n2.gbif.org:2181,c1n3.gbif.org:2181/solr5dev collection --create tim-untyped -s 3 -c tim-untyped-test -r 1 -m 3
