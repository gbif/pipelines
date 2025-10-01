package org.gbif.pipelines.transforms.extension;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.HUMBOLDT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.HUMBOLDT_RECORDS_COUNT;
import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.extension.HumboldtInterpreter;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.HumboldtRecord;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/**
 * Beam level transformations for the Humboldt extension, reads an avro, writes an avro, maps from
 * value to keyValue and transforms form {@link ExtendedRecord} to {@link HumboldtRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link HumboldtRecord} using {@link ExtendedRecord}
 * as a source and {@link HumboldtInterpreter} as interpretation steps
 *
 * @see <a href="http://rs.tdwg.org/eco/terms/Event">Extension definition</a>
 */
public class HumboldtTransform extends Transform<ExtendedRecord, HumboldtRecord> {

  private final SerializableSupplier<VocabularyService> vocabularyServiceSupplier;
  private final SerializableSupplier<KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>>
      nameUsageMatchKvStoreSupplier;
  private final List<String> checklistKeys;

  private KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> nameUsageMatchKvStore;
  private VocabularyService vocabularyService;

  private HumboldtInterpreter humboldtInterpreter;

  @Builder(buildMethodName = "create")
  private HumboldtTransform(
      SerializableSupplier<VocabularyService> vocabularyServiceSupplier,
      SerializableSupplier<KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>>
          nameUsageMatchKvStoreSupplier,
      List<String> checklistKeys) {
    super(
        HumboldtRecord.class, HUMBOLDT, HumboldtTransform.class.getName(), HUMBOLDT_RECORDS_COUNT);
    this.vocabularyServiceSupplier = vocabularyServiceSupplier;
    this.nameUsageMatchKvStoreSupplier = nameUsageMatchKvStoreSupplier;
    this.checklistKeys = checklistKeys;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    Objects.requireNonNull(nameUsageMatchKvStoreSupplier);
    Objects.requireNonNull(vocabularyServiceSupplier);
    if (humboldtInterpreter == null) {
      nameUsageMatchKvStore = nameUsageMatchKvStoreSupplier.get();
      vocabularyService = vocabularyServiceSupplier.get();

      humboldtInterpreter =
          HumboldtInterpreter.builder()
              .kvStore(nameUsageMatchKvStore)
              .checklistKeys(checklistKeys)
              .vocabularyService(vocabularyService)
              .create();
    }
  }

  /** Beam @Teardown closes initialized resources */
  @SneakyThrows
  @Teardown
  public void tearDown() {
    if (vocabularyService != null) {
      vocabularyService.close();
    }
    if (nameUsageMatchKvStore != null) {
      nameUsageMatchKvStore.close();
    }
  }

  /** Beam @Setup can be applied only to void method */
  public HumboldtTransform init() {
    setup();
    return this;
  }

  /** Maps {@link HumboldtRecord} to key value, where key is {@link HumboldtRecord#getId} */
  public MapElements<HumboldtRecord, KV<String, HumboldtRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, HumboldtRecord>>() {})
        .via((HumboldtRecord hr) -> KV.of(hr.getId(), hr));
  }

  public HumboldtTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<HumboldtRecord> convert(ExtendedRecord source) {
    return Interpretation.from(source)
        .to(
            er ->
                HumboldtRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> hasExtension(er, Extension.HUMBOLDT))
        .via(humboldtInterpreter::interpret)
        .getOfNullable();
  }
}
