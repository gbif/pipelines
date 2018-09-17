--
-- A script to set up Hive tables in the pipelines database, backed by avro files.
--

USE pipelines;

SET hive.variable.substitute=true;

DROP TABLE IF EXISTS occurrence_raw;
DROP TABLE IF EXISTS occurrence_interpreted;
DROP TABLE IF EXISTS occurrence_issues;

SET raw_avro_schema_path="hdfs://ha-nn/pipelines/avroschemas/dwca.avsc";
SET interpreted_avro_schema_path="hdfs://ha-nn/pipelines/avroschemas/interpreted.avsc";
SET issues_avro_schema_path="hdfs://ha-nn/pipelines/avroschemas/issues.avsc";
SET avro_interpreted_data_location="/pipelines/avrotest1/interpreted";
SET avro_raw_data_location="/pipelines/avrotest1/raw";
SET avro_issues_data_location="/pipelines/avrotest1/issues";

CREATE EXTERNAL TABLE occurrence_raw
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${hiveconf:avro_raw_data_location}'
TBLPROPERTIES ('avro.schema.url'= 'hdfs://ha-nn/pipelines/avroschemas/dwca.avsc');

CREATE EXTERNAL TABLE occurrence_interpreted
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION "${hiveconf:avro_interpreted_data_location}"
TBLPROPERTIES ('avro.schema.url'= 'hdfs://ha-nn/pipelines/avroschemas/interpreted.avsc');

CREATE EXTERNAL TABLE occurrence_issues
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${hiveconf:avro_issues_data_location}'
TBLPROPERTIES ('avro.schema.url'= 'hdfs://ha-nn/pipelines/avroschemas/issues.avsc');
