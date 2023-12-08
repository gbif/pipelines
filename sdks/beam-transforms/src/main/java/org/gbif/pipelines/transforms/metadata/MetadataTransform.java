package org.gbif.pipelines.transforms.metadata;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.METADATA;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.METADATA_RECORDS_COUNT;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.metadata.MetadataInterpreter;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.Transform;

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
  private final SerializableSupplier<MetadataServiceClient> clientSupplier;
  private MetadataServiceClient client;

  @Builder(buildMethodName = "create")
  private MetadataTransform(
      Integer attempt, SerializableSupplier<MetadataServiceClient> clientSupplier) {
    super(
        MetadataRecord.class, METADATA, MetadataTransform.class.getName(), METADATA_RECORDS_COUNT);
    this.attempt = attempt;
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

  /** Beam @Setup can be applied only to void method * */
  public MetadataTransform init() {
    setup();
    return this;
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
        .getOfNullable();
  }

  /** Checks if list contains metadata type only */
  public boolean metadataOnly(Set<String> types) {
    return types.size() == 1 && types.contains(getRecordType().name());
  }
}
