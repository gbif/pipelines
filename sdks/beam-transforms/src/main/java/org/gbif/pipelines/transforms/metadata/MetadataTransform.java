package org.gbif.pipelines.transforms.metadata;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.METADATA_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.METADATA;
import static org.gbif.pipelines.transforms.common.CheckTransforms.checkRecordType;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.metadata.MetadataInterpreter;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.pipelines.transforms.common.CheckTransforms;

/**
 * Beam level transformations for the GBIF metadata, reads an avro, writes an avro, maps from value
 * to keyValue and transforms form {@link ExtendedRecord} to {@link MetadataRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link MetadataRecord} using {@link ExtendedRecord}
 * as a source and {@link MetadataInterpreter} as interpretation steps
 *
 * <p>
 */
@Slf4j
public class MetadataTransform extends Transform<String, MetadataRecord> {

  private final Integer attempt;
  private final String endpointType;
  private final SerializableSupplier<MetadataServiceClient> clientSupplier;
  private MetadataServiceClient client;

  @Builder(buildMethodName = "create")
  private MetadataTransform(
      Integer attempt,
      String endpointType,
      SerializableSupplier<MetadataServiceClient> clientSupplier) {
    super(
        MetadataRecord.class, METADATA, MetadataTransform.class.getName(), METADATA_RECORDS_COUNT);
    this.attempt = attempt;
    this.endpointType = endpointType;
    this.clientSupplier = clientSupplier;
  }

  public MetadataTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (client == null && clientSupplier != null) {
      log.info("Initialize MetadataServiceClient");
      client = clientSupplier.get();
    }
  }

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {
    if (client != null) {
      log.info("Close MetadataServiceClient");
      client.close();
    }
  }

  @Override
  public Optional<MetadataRecord> convert(String source) {
    return Interpretation.from(source)
        .to(
            id ->
                MetadataRecord.newBuilder()
                    .setId(id)
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .via(MetadataInterpreter.interpret(client))
        .via(MetadataInterpreter.interpretCrawlId(attempt))
        .via(MetadataInterpreter.interpretEndpointType(endpointType))
        .getOfNullable();
  }

  /**
   * Checks if list contains {@link MetadataTransform#getRecordType()}, else returns empty {@link
   * PCollection<MetadataRecord>}
   */
  public CheckTransforms<MetadataRecord> checkMetadata(Set<String> types) {
    return CheckTransforms.create(MetadataRecord.class, checkRecordType(types, getRecordType()));
  }

  /** Checks if list contains metadata type only */
  public boolean metadataOnly(Set<String> types) {
    return types.size() == 1 && types.contains(getRecordType().name());
  }
}
