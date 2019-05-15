# GBIF Deployments

During development, we are deploying APIs backed by the pipelines project in parallel with the existing HBase/SOLR-based system.

The pipelines/ElasticSearch branches of the existing webservices: occurrence-ws, vectortile-server and mapnik-server have had `es-` prepended to their artifact names, and are modified (for the first two) and reconfigured (for all) to use pipelines data.  These services are released and deployed from Jenkins, and monitored using Nagios, in the usual way.

There are then two additional hostnames, which can be used to see data from either system:

* https://api-es.gbif.org/, backed by pipelines systems (and also https://api-es.gbif-uat.org/ and https://api-es.gbif-dev.org/)
* https://api-solr.gbif.org/, backed by the existing system (and also https://api-solr.gbif-uat.org/ and https://api-solr.gbif-dev.org/)

Similarly, there are two additional portals, configured appropriately and deployed using Jenkins:

* https://www-es.gbif.org/, https://www-es.gbif-uat.org/ or https://www-es.gbif-dev.org/
* https://www-solr.gbif.org/, https://www-solr.gbif-uat.org/ or https://www-solr.gbif-dev.org/

These are all temporary, just for testing during the transition to pipelines.

The default backend is set using Varnish configuration.  Initially, https://api.gbif…/ and https://www.gbif…/ will use the existing system.  This can later be changed, and then changed back quickly in case of a pipelines system failure or data corruption.
