package org.gbif.pipelines.spark;

/** Common command line arguments used by several pipelines. */
public interface ArgsConstants {
  String DATASET_TYPE_ARG = "--datasetType";
  String SOURCE_DIRECTORY_ARG = "--sourceDirectory";
  String APP_NAME_ARG = "--appName";
  String DATASET_ID_ARG = "--datasetId";
  String ATTEMPT_ID_ARG = "--attempt";
  String CONFIG_PATH_ARG = "--config";
  String SPARK_MASTER_ARG = "--master";
  String NUMBER_OF_SHARDS_ARG = "--numberOfShards";
  String SWITCH_ON_SUCCESS = "--switchOnSuccess";
  String UNSUCCESSFUL_DUMP_FILENAME = "--unsuccessfulDumpFilename";
}
