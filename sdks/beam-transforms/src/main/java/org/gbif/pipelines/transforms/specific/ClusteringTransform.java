package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.CLUSTERING_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.CLUSTERING;

import java.time.Instant;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.specific.ClusteredInterpreter;
import org.gbif.pipelines.core.parsers.clustering.ClusteringService;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Use GBIF identifier from {@link IdentifierRecord} to get clustering value for {@link
 * ClusteringRecord}.
 */
public class ClusteringTransform extends Transform<IdentifierRecord, ClusteringRecord> {

  private final SerializableSupplier<ClusteringService> clusteringServiceSupplier;

  private ClusteringService clusteringService;

  @Builder(buildMethodName = "create")
  private ClusteringTransform(SerializableSupplier<ClusteringService> clusteringServiceSupplier) {
    super(
        ClusteringRecord.class,
        CLUSTERING,
        ClusteringTransform.class.getName(),
        CLUSTERING_RECORDS_COUNT);
    this.clusteringServiceSupplier = clusteringServiceSupplier;
  }

  /** Maps {@link ClusteringRecord} to key value, where key is {@link ClusteringRecord#getId} */
  public MapElements<ClusteringRecord, KV<String, ClusteringRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ClusteringRecord>>() {})
        .via((ClusteringRecord cr) -> KV.of(cr.getId(), cr));
  }

  public ClusteringTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (clusteringServiceSupplier != null) {
      clusteringService = clusteringServiceSupplier.get();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public ClusteringTransform init() {
    setup();
    return this;
  }

  @Override
  public Optional<ClusteringRecord> convert(IdentifierRecord source) {

    return Interpretation.from(source)
        .to(
            ir ->
                ClusteringRecord.newBuilder()
                    .setId(ir.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(ir -> ir.getInternalId() != null)
        .via(ClusteredInterpreter.interpretIsClustered(clusteringService))
        .getOfNullable();
  }
}
