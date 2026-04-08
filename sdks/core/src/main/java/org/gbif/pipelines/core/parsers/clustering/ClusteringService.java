package org.gbif.pipelines.core.parsers.clustering;

import com.fasterxml.jackson.core.JsonParseException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.config.model.ClusteringRelationshipConfig;

@Slf4j
@SuppressWarnings("all")
public class ClusteringService implements Serializable, Closeable {

  private final Connection connection;
  private final ClusteringRelationshipConfig config;
  private final Retry retry;

  private ClusteringService(Connection connection, ClusteringRelationshipConfig config) {
    this.connection = connection;
    this.config = config;
    this.retry =
        Retry.of(
            "clusteringCall",
            RetryConfig.custom()
                .maxAttempts(config.getRetryMaxAttempts())
                .retryExceptions(
                    JsonParseException.class,
                    IOException.class,
                    TimeoutException.class,
                    PipelinesException.class)
                .intervalFunction(
                    IntervalFunction.ofExponentialBackoff(
                        Duration.ofSeconds(config.getRetryDuration())))
                .build());
  }

  public static ClusteringService create(
      Connection connection, ClusteringRelationshipConfig config) {
    return new ClusteringService(connection, config);
  }

  public boolean isClustered(Long gbifId) {

    Supplier<Boolean> fn =
        () -> {
          try (Table table =
              connection.getTable(TableName.valueOf(config.getRelationshipTableName()))) {
            Scan scan = new Scan();
            scan.setBatch(1);
            scan.addFamily(Bytes.toBytes("o"));
            int salt = Math.abs(gbifId.toString().hashCode()) % config.getRelationshipTableSalt();
            scan.setRowPrefixFilter(Bytes.toBytes(salt + ":" + gbifId + ":"));
            ResultScanner s = table.getScanner(scan);
            Result row = s.next();
            return row != null;
          } catch (IOException ex) {
            log.error(ex.getMessage(), ex);
            throw new PipelinesException(ex);
          }
        };

    return retry.executeSupplier(fn);
  }

  @Override
  public void close() throws IOException {
    if (connection != null) {
      connection.close();
    }
  }
}
