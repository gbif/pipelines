package au.org.ala.pipelines.transforms;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.METADATA;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.METADATA_RECORDS_COUNT;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.interpreters.ALAAttributionInterpreter;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.Transform;

@Slf4j
public class MetadataTransform extends Transform<String, MetadataRecord> {

  private final SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
      dataResourceKvStoreSupplier;
  private KeyValueStore<String, ALACollectoryMetadata> kvStore;
  private final String datasetId;

  @Builder(buildMethodName = "create")
  private MetadataTransform(
      SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
          dataResourceKvStoreSupplier,
      String datasetId) {
    super(MetadataRecord.class, METADATA, MetadataRecord.class.getName(), METADATA_RECORDS_COUNT);
    this.dataResourceKvStoreSupplier = dataResourceKvStoreSupplier;
    this.datasetId = datasetId;
  }

  public MetadataTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (kvStore == null && dataResourceKvStoreSupplier != null) {
      log.info("Initialize DataResource KV store");
      kvStore = dataResourceKvStoreSupplier.get();
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
        .to(id -> MetadataRecord.newBuilder().setId(id).build())
        .via(ALAAttributionInterpreter.interpretDatasetKeyAsMetadataRecord(datasetId, kvStore))
        .getOfNullable();
  }
}
