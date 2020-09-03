package org.gbif.pipelines.hbase.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.gbif.pipelines.hbase.options.ExportHBaseOptions;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConfigurationFactory {

  @SneakyThrows
  public static Configuration create(ExportHBaseOptions options) {
    Configuration hbaseConf = HBaseConfiguration.create();
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, options.getHbaseZk());
    hbaseConf.set("hbase.rootdir", "/hbase");
    hbaseConf.setClass(
        "mapreduce.job.inputformat.class", TableSnapshotInputFormat.class, InputFormat.class);
    hbaseConf.setClass("key.class", ImmutableBytesWritable.class, Writable.class);
    hbaseConf.setClass("value.class", Result.class, Object.class);

    Scan scan = new Scan();
    scan.setBatch(options.getBatchSize()); // for safety
    scan.addFamily("o".getBytes());
    ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
    hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray()));

    // Make use of existing utility methods
    Job job = Job.getInstance(hbaseConf); // creates internal clone of hbaseConf
    TableSnapshotInputFormat.setInput(job, options.getTable(), new Path(options.getRestoreDir()));
    return job.getConfiguration(); // extract the modified clone
  }
}
