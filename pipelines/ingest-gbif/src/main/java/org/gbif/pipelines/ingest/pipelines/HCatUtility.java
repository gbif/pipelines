package org.gbif.pipelines.ingest.pipelines;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.thrift.TException;

public class HCatUtility {


  public static void main(String[] args) throws IOException, TException {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://c5master1-vh.gbif.org:10002");
    HiveMetaStoreClient hiveMetaStoreClient = HCatUtil.getHiveClient(hiveConf);
    HCatSchema hCatSchema = new HCatSchema(HCatUtil.getHCatFieldSchemaList(hiveMetaStoreClient.getSchema("prod_g","occurrence_hdfs")));
    System.out.println(hCatSchema);
  }
}
