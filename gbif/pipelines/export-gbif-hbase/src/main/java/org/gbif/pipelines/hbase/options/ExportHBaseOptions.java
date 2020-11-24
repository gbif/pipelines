package org.gbif.pipelines.hbase.options;

import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Pipeline settings and arguments for Hbase to Avro export. */
public interface ExportHBaseOptions extends HadoopFileSystemOptions {

  @Description("HBase Zookeeper ensemble")
  String getHbaseZk();

  void setHbaseZk(String hbaseZk);

  @Description("Export path of avro files")
  String getExportPath();

  void setExportPath(String exportPath);

  @Description("Batch size of documents to be read from HBase")
  @Default.Integer(10000)
  int getBatchSize();

  void setBatchSize(int batchSize);

  @Description("Occurrence table")
  String getTable();

  void setTable(String table);

  @Description("Temporary directory to restore the snapshot into")
  @Default.String("/tmp/snapshot_restore")
  String getRestoreDir();

  void setRestoreDir(String restoreDir);
}
