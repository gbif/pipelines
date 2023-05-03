package au.org.ala.pipelines.transforms;

import static au.org.ala.pipelines.common.ALARecordTypes.RECORD_ANNOTATION;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.METADATA_RECORDS_COUNT;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.JackKnifeOutlierRecord;
import org.gbif.pipelines.io.avro.RecordAnnotation;
import org.gbif.pipelines.transforms.Transform;

@Slf4j
public class RecordAnnotationTransform extends Transform<ExtendedRecord, RecordAnnotation> {

  private final SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
      dataResourceKvStoreSupplier;
  private KeyValueStore<String, ALACollectoryMetadata> kvStore;
  private final String datasetId;

  @Builder(buildMethodName = "create")
  private RecordAnnotationTransform(
      SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
          dataResourceKvStoreSupplier,
      String datasetId) {
    super(
        RecordAnnotation.class,
        RECORD_ANNOTATION,
        RecordAnnotation.class.getName(),
        METADATA_RECORDS_COUNT);

    this.dataResourceKvStoreSupplier = dataResourceKvStoreSupplier;
    this.datasetId = datasetId;
  }

  /**
   * Maps {@link JackKnifeOutlierRecord} to key value, where key is {@link
   * JackKnifeOutlierRecord#getId}
   */
  public MapElements<RecordAnnotation, KV<String, RecordAnnotation>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, RecordAnnotation>>() {})
        .via((RecordAnnotation lr) -> KV.of(lr.getId(), lr));
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {}

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {}

  @Override
  public Optional<RecordAnnotation> convert(ExtendedRecord source) {

    // add DOIs
    ALACollectoryMetadata alaCollectoryMetadata = dataResourceKvStoreSupplier.get().get(datasetId);

    // needs to be the ALA UUID
    String occurrenceID = source.getCoreTerms().get(DwcTerm.occurrenceID.qualifiedName());
    String scientificName = source.getCoreTerms().get(DwcTerm.scientificName.qualifiedName());
    String decimalLatitude = source.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName());
    String decimalLongitude = source.getCoreTerms().get(DwcTerm.decimalLongitude.qualifiedName());
    String year = source.getCoreTerms().get(DwcTerm.year.qualifiedName());
    String month = source.getCoreTerms().get(DwcTerm.month.qualifiedName());
    String occurrenceRemarks = source.getCoreTerms().get(DwcTerm.occurrenceRemarks.qualifiedName());

    if (occurrenceID != null) {
      RecordAnnotation recordAnnotation =
          RecordAnnotation.newBuilder()
              .setId(occurrenceID)
              .setDoi(alaCollectoryMetadata.getDoi())
              .setDatasetKey(datasetId)
              .setScientificName(scientificName)
              .setDecimalLatitude(decimalLatitude)
              .setDecimalLongitude(decimalLongitude)
              .setYear(year)
              .setMonth(month)
              .setOccurrenceRemarks(occurrenceRemarks)
              .build();
      return Optional.of(recordAnnotation);
    }
    return Optional.empty();
  }
}
