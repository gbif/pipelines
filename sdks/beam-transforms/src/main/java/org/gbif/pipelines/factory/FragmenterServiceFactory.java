package org.gbif.pipelines.factory;

import java.io.IOException;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.config.model.KeygenConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.keygen.common.HbaseConnection;
import org.gbif.pipelines.keygen.common.HbaseConnectionFactory;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FragmenterServiceFactory {

  public static SerializableSupplier<Table> getInstanceSupplier(PipelinesConfig config) {
    return () -> {
      String zk = getZk(config);

      Connection c = HbaseConnectionFactory.getInstance(zk, getHBaseZnode(config)).getConnection();

      return create(config, c);
    };
  }

  public static SerializableSupplier<Table> createSupplier(PipelinesConfig config) {
    return () -> {
      String zk = getZk(config);

      Connection c;
      try {
        c = HbaseConnection.create(zk, getHBaseZnode(config));
      } catch (IOException ex) {
        throw new PipelinesException(ex);
      }

      return create(config, c);
    };
  }

  @SneakyThrows
  private static Table create(PipelinesConfig config, Connection c) {
    return c.getTable(TableName.valueOf(config.getFragmentsTable()));
  }

  private static String getZk(PipelinesConfig config) {
    return Optional.ofNullable(config.getKeygen())
        .map(KeygenConfig::getZkConnectionString)
        .filter(x -> !x.isEmpty())
        .orElse(config.getZkConnectionString());
  }

  private static String getHBaseZnode(PipelinesConfig config) {
    return Optional.ofNullable(config.getKeygen())
        .map(KeygenConfig::getHbaseZnode)
        .filter(x -> !x.isEmpty())
        .orElse("/hbase");
  }
}
