package org.gbif.pipelines.keygen.common;

import com.google.common.base.Strings;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HbaseConnection {

  public static Connection create(String hbaseZk, String zNodeParent) throws IOException {
    if (Strings.isNullOrEmpty(hbaseZk)) {
      return ConnectionFactory.createConnection(HBaseConfiguration.create());
    }
    Configuration hbaseConfig = HBaseConfiguration.create();
    hbaseConfig.set("hbase.zookeeper.quorum", hbaseZk);
    hbaseConfig.set("zookeeper.session.timeout", "360000");
    if (!Strings.isNullOrEmpty(zNodeParent)) {
      hbaseConfig.set("zookeeper.znode.parent", zNodeParent);
    }
    return ConnectionFactory.createConnection(hbaseConfig);
  }

  public static Connection create() throws IOException {
    return create(null, null);
  }
}
