package au.org.ala.pipelines.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.BASIC_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.BASIC;

import au.org.ala.pipelines.interpreters.ALABasicInterpreter;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.BasicInterpreter;
import org.gbif.pipelines.core.interpreters.core.CoreInterpreter;
import org.gbif.pipelines.core.interpreters.core.VocabularyInterpreter;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformations for the DWC Occurrence, reads an avro, writs an avro, maps from value
 * to keyValue and transforms form {@link ExtendedRecord} to {@link BasicRecord}.
 *
 * @see <a href="https://dwc.tdwg.org/terms/#occurrence</a>
 */
public class ALABasicTransform extends Transform<ExtendedRecord, BasicRecord> {

  private final SerializableSupplier<KeyValueStore<String, OccurrenceStatus>>
      occStatusKvStoreSupplier;
  private final SerializableSupplier<KeyValueStore<String, List<String>>> recordedByKvStoreSupplier;
  private final SerializableSupplier<VocabularyService> vocabularyServiceSupplier;

  private KeyValueStore<String, OccurrenceStatus> occStatusKvStore;
  private KeyValueStore<String, List<String>> recordedByKvStore;
  private VocabularyService vocabularyService;

  @Builder(buildMethodName = "create")
  private ALABasicTransform(
      SerializableSupplier<KeyValueStore<String, OccurrenceStatus>> occStatusKvStoreSupplier,
      SerializableSupplier<KeyValueStore<String, List<String>>> recordedByKvStoreSupplier,
      SerializableSupplier<VocabularyService> vocabularyServiceSupplier) {
    super(BasicRecord.class, BASIC, ALABasicTransform.class.getName(), BASIC_RECORDS_COUNT);
    this.occStatusKvStoreSupplier = occStatusKvStoreSupplier;
    this.recordedByKvStoreSupplier = recordedByKvStoreSupplier;
    this.vocabularyServiceSupplier = vocabularyServiceSupplier;
  }

  /** Maps {@link BasicRecord} to key value, where key is {@link BasicRecord#getId} */
  public MapElements<BasicRecord, KV<String, BasicRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, BasicRecord>>() {})
        .via((BasicRecord br) -> KV.of(br.getId(), br));
  }

  public ALABasicTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (occStatusKvStore == null && occStatusKvStoreSupplier != null) {
      occStatusKvStore = occStatusKvStoreSupplier.get();
    }
    if (recordedByKvStore == null && recordedByKvStoreSupplier != null) {
      recordedByKvStore = recordedByKvStoreSupplier.get();
    }
    if (vocabularyService == null && vocabularyServiceSupplier != null) {
      vocabularyService = vocabularyServiceSupplier.get();
    }
  }

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {
    // NOP
  }

  @Override
  public Optional<BasicRecord> convert(ExtendedRecord source) {

    BasicRecord br =
        BasicRecord.newBuilder()
            .setId(source.getId())
            .setCreated(Instant.now().toEpochMilli())
            .build();

    return Interpretation.from(source)
        .to(br)
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(BasicInterpreter::interpretBasisOfRecord)
        .via(BasicInterpreter::interpretTypifiedName)
        .via(BasicInterpreter::interpretSex)
        .via(BasicInterpreter::interpretTypeStatus)
        .via(BasicInterpreter::interpretIndividualCount)
        .via((e, r) -> CoreInterpreter.interpretReferences(e, r, r::setReferences))
        .via(BasicInterpreter::interpretOrganismQuantity)
        .via(BasicInterpreter::interpretOrganismQuantityType)
        .via((e, r) -> CoreInterpreter.interpretSampleSizeUnit(e, r::setSampleSizeUnit))
        .via((e, r) -> CoreInterpreter.interpretSampleSizeValue(e, r::setSampleSizeValue))
        .via(BasicInterpreter::interpretRelativeOrganismQuantity)
        .via(BasicInterpreter::interpretIdentifiedByIds)
        .via(BasicInterpreter::interpretRecordedByIds)
        .via(BasicInterpreter.interpretOccurrenceStatus(occStatusKvStore))
        .via(VocabularyInterpreter.interpretEstablishmentMeans(vocabularyService))
        .via(VocabularyInterpreter.interpretLifeStage(vocabularyService))
        .via(ALABasicInterpreter::interpretLicense)
        .via(ALABasicInterpreter.interpretRecordedBy(recordedByKvStore))
        .via((e, r) -> CoreInterpreter.interpretDatasetID(e, r::setDatasetID))
        .via((e, r) -> CoreInterpreter.interpretDatasetName(e, r::setDatasetName))
        .via(BasicInterpreter::interpretOtherCatalogNumbers)
        .via(BasicInterpreter::interpretIdentifiedBy)
        .via(BasicInterpreter::interpretPreparations)
        .via((e, r) -> CoreInterpreter.interpretSamplingProtocol(e, r::setSamplingProtocol))
        .via(BasicInterpreter::setCoreId)
        .getOfNullable();
  }
}
