# This script is on one of the nodes of the cassandra cluster (e.g. aws-cass-cluster-1b.ala
# and will write to a directory on the server
rm -f /data/uuid-exports/*.csv
rm -f /data/uuid-exports/*.gz
cqlsh -k occ -e "COPY occ_uuid TO '/data/uuid-exports/occ_uuid.csv'"
cqlsh -k occ -e "COPY occ (rowkey, \"firstLoaded\") TO '/data/uuid-exports/occ_first_loaded_date.csv';"
gzip /data/uuid-exports/*.csv