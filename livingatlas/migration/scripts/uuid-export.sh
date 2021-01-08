cqlsh -k occ -e "COPY occ_uuid TO '/data/uuid-exports/occ_uuid.csv'"
cqlsh -k occ -e "COPY occ (rowkey, \"firstLoaded\") TO '/data/uuid-exports/occ_first_loaded_date.csv';"