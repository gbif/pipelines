package org.gbif.pipelines.interpretation.transform;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.interpreters.specific.ClusteredInterpreter;
import org.gbif.pipelines.core.parsers.clustering.ClusteringService;
import org.gbif.pipelines.interpretation.transform.utils.ClusteringServiceFactory;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;

public class ClusteringTransform implements Serializable, Closeable {

  private final PipelinesConfig config;
  private transient ClusteringService clusteringService;

  private ClusteringTransform(PipelinesConfig config) {
    this.config = config;
  }

  public static ClusteringTransform create(PipelinesConfig config) {
    return new ClusteringTransform(config);
  }

  public ClusteringRecord convert(IdentifierRecord source) {

    ClusteringRecord cr =
        ClusteringRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    if (clusteringService == null) {
      this.clusteringService = ClusteringServiceFactory.createSupplier(config).get();
    }

    ClusteredInterpreter.interpretIsClustered(clusteringService).accept(source, cr);
    return cr;
  }

  @Override
  public void close() throws IOException {
    if (clusteringService != null) {
      this.clusteringService.close();
    }
  }
}
