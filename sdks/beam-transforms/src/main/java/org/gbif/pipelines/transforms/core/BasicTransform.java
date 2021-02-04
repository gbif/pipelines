package org.gbif.pipelines.transforms.core;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.BASIC_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.BASIC;
import static org.gbif.pipelines.core.interpreters.core.BasicInterpreter.GBIF_ID_INVALID;
import static org.gbif.pipelines.core.interpreters.core.BasicInterpreter.interpretCopyGbifId;

import java.time.Instant;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.BasicInterpreter;
import org.gbif.pipelines.core.interpreters.core.DynamicPropertiesInterpreter;
import org.gbif.pipelines.core.parsers.clustering.ClusteringService;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.transforms.Transform;
import org.gbif.vocabulary.lookup.LookupConcept;
import org.gbif.vocabulary.lookup.VocabularyLookup;

/**
 * Beam level transformations for the DWC Occurrence, reads an avro, writs an avro, maps from value
 * to keyValue and transforms form {@link ExtendedRecord} to {@link BasicRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#occurrence</a>
 */
public class BasicTransform extends Transform<ExtendedRecord, BasicRecord> {

  private final boolean isTripletValid;
  private final boolean isOccurrenceIdValid;
  private final boolean useExtendedRecordId;
  private final BiConsumer<ExtendedRecord, BasicRecord> gbifIdFn;
  private final SerializableSupplier<HBaseLockingKeyService> keygenServiceSupplier;
  private final SerializableSupplier<VocabularyLookup> lifeStageLookupSupplier;
  private final SerializableSupplier<KeyValueStore<String, OccurrenceStatus>>
      occStatusKvStoreSupplier;
  private final SerializableSupplier<ClusteringService> clusteringServiceSupplier;

  private KeyValueStore<String, OccurrenceStatus> occStatusKvStore;
  private HBaseLockingKeyService keygenService;
  private VocabularyLookup lifeStageLookup;
  private ClusteringService clusteringService;

  private Function<String, Optional<LookupConcept>> lifeStageLookupFn;

  @Builder(buildMethodName = "create")
  private BasicTransform(
      boolean isTripletValid,
      boolean isOccurrenceIdValid,
      boolean useExtendedRecordId,
      BiConsumer<ExtendedRecord, BasicRecord> gbifIdFn,
      SerializableSupplier<HBaseLockingKeyService> keygenServiceSupplier,
      SerializableSupplier<VocabularyLookup> lifeStageLookupSupplier,
      SerializableSupplier<KeyValueStore<String, OccurrenceStatus>> occStatusKvStoreSupplier,
      SerializableSupplier<ClusteringService> clusteringServiceSupplier) {
    super(BasicRecord.class, BASIC, BasicTransform.class.getName(), BASIC_RECORDS_COUNT);
    this.isTripletValid = isTripletValid;
    this.isOccurrenceIdValid = isOccurrenceIdValid;
    this.useExtendedRecordId = useExtendedRecordId;
    this.gbifIdFn = gbifIdFn;
    this.keygenServiceSupplier = keygenServiceSupplier;
    this.occStatusKvStoreSupplier = occStatusKvStoreSupplier;
    this.lifeStageLookupSupplier = lifeStageLookupSupplier;
    this.clusteringServiceSupplier = clusteringServiceSupplier;
  }

  /** Maps {@link BasicRecord} to key value, where key is {@link BasicRecord#getId} */
  public MapElements<BasicRecord, KV<String, BasicRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, BasicRecord>>() {})
        .via((BasicRecord br) -> KV.of(br.getId(), br));
  }

  /** Maps {@link BasicRecord} to key value, where key is {@link BasicRecord#getGbifId()} */
  public MapElements<BasicRecord, KV<String, BasicRecord>> toGbifIdKv() {
    return MapElements.into(new TypeDescriptor<KV<String, BasicRecord>>() {})
        .via(
            (BasicRecord br) -> {
              String key =
                  Optional.ofNullable(br.getGbifId()).map(Object::toString).orElse(GBIF_ID_INVALID);
              return KV.of(key, br);
            });
  }

  public BasicTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (keygenService == null && keygenServiceSupplier != null) {
      keygenService = keygenServiceSupplier.get();
    }
    if (occStatusKvStore == null && occStatusKvStoreSupplier != null) {
      occStatusKvStore = occStatusKvStoreSupplier.get();
    }
    if (lifeStageLookupSupplier != null) {
      lifeStageLookup = lifeStageLookupSupplier.get();
      if (lifeStageLookup != null) {
        lifeStageLookupFn = lifeStageLookup::lookup;
      }
    }
    if (clusteringServiceSupplier != null) {
      clusteringService = clusteringServiceSupplier.get();
    }
  }

  /** Beam @Teardown closes initialized resources */
  @SneakyThrows
  @Teardown
  public void tearDown() {
    if (keygenService != null) {
      keygenService.close();
    }
    if (lifeStageLookup != null) {
      lifeStageLookup.close();
    }
  }

  @Override
  public Optional<BasicRecord> convert(ExtendedRecord source) {

    BasicRecord br =
        BasicRecord.newBuilder()
            .setId(source.getId())
            .setGbifId(
                useExtendedRecordId && source.getCoreTerms().isEmpty()
                    ? Long.parseLong(source.getId())
                    : null)
            .setCreated(Instant.now().toEpochMilli())
            .build();

    if (useExtendedRecordId && source.getCoreTerms().isEmpty()) {
      interpretCopyGbifId().accept(source, br);
    }

    return Interpretation.from(source)
        .to(br)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(
            BasicInterpreter.interpretGbifId(
                keygenService, isTripletValid, isOccurrenceIdValid, useExtendedRecordId, gbifIdFn))
        .via(BasicInterpreter::interpretBasisOfRecord)
        .via(BasicInterpreter::interpretTypifiedName)
        .via(BasicInterpreter::interpretSex)
        .via(BasicInterpreter::interpretEstablishmentMeans)
        .via(BasicInterpreter.interpretLifeStage(lifeStageLookupFn))
        .via(BasicInterpreter::interpretTypeStatus)
        .via(BasicInterpreter::interpretIndividualCount)
        .via(BasicInterpreter::interpretReferences)
        .via(BasicInterpreter::interpretOrganismQuantity)
        .via(BasicInterpreter::interpretOrganismQuantityType)
        .via(BasicInterpreter::interpretSampleSizeUnit)
        .via(BasicInterpreter::interpretSampleSizeValue)
        .via(BasicInterpreter::interpretRelativeOrganismQuantity)
        .via(BasicInterpreter::interpretLicense)
        .via(BasicInterpreter::interpretIdentifiedByIds)
        .via(BasicInterpreter::interpretRecordedByIds)
        .via(BasicInterpreter.interpretOccurrenceStatus(occStatusKvStore))
        .via(BasicInterpreter.interpretIsClustered(clusteringService))
        .via(DynamicPropertiesInterpreter::interpretPreparations)
        .via(DynamicPropertiesInterpreter::interpretSex)
        .via(DynamicPropertiesInterpreter.interpretLifeStage(lifeStageLookupFn))
        .getOfNullable();
  }
}
