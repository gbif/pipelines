package au.org.ala.pipelines.transforms;

import static au.org.ala.pipelines.common.ALARecordTypes.ALA_SENSITIVE_DATA;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.interpreters.SensitiveDataInterpreter;
import au.org.ala.sds.api.ConservationApi;
import au.org.ala.sds.api.SpeciesCheck;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ALASensitivityRecord;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Perform transformations on sensitive data.
 *
 * <p>Beam level transformations for the DWC Taxon, reads an avro, writes an avro, maps from value
 * to keyValue and transforms form {@link ExtendedRecord} to {@link TaxonRecord}.
 *
 * <p>ParDo runs sequence of interpretations for {@link TaxonRecord} using {@link ExtendedRecord} as
 * a source and {@link TaxonomyInterpreter} as interpretation steps
 *
 * @see <a href="https://dwc.tdwg.org/terms/#taxon</a>
 */
@Slf4j
public class ALASensitiveDataTransform
    extends Transform<KV<String, CoGbkResult>, ALASensitivityRecord> {

  private final String datasetId;
  private KeyValueStore<SpeciesCheck, Boolean> speciesStore;
  private final SerializableSupplier<KeyValueStore<SpeciesCheck, Boolean>> speciesStoreSupplier;
  private KeyValueStore<String, ALACollectoryMetadata> dataResourceStore;
  private final SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
      dataResourceStoreSupplier;
  private ConservationApi conservationService;
  private final SerializableSupplier<ConservationApi> conservationServiceSupplier;
  private Map<String, Term> sensitiveFields;
  @NonNull private final TupleTag<ExtendedRecord> erTag;
  @NonNull private final TupleTag<TemporalRecord> trTag;
  @NonNull private final TupleTag<TaxonRecord> txrTag;
  @NonNull private final TupleTag<ALATaxonRecord> atxrTag;
  @NonNull private final TupleTag<LocationRecord> lrTag;

  @Builder(buildMethodName = "create")
  private ALASensitiveDataTransform(
      String datasetId,
      KeyValueStore<SpeciesCheck, Boolean> speciesStore,
      SerializableSupplier<KeyValueStore<SpeciesCheck, Boolean>> speciesStoreSupplier,
      KeyValueStore<String, ALACollectoryMetadata> dataResourceStore,
      SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>> dataResourceStoreSupplier,
      ConservationApi conservationService,
      SerializableSupplier<ConservationApi> conservationServiceSupplier,
      TupleTag<ExtendedRecord> erTag,
      TupleTag<TemporalRecord> trTag,
      TupleTag<LocationRecord> lrTag,
      TupleTag<TaxonRecord> txrTag,
      TupleTag<ALATaxonRecord> atxrTag) {
    super(
        ALASensitivityRecord.class,
        ALA_SENSITIVE_DATA,
        ALASensitiveDataTransform.class.getName(),
        "alaSensitiveDataRecordCount");
    this.datasetId = datasetId;
    this.speciesStore = speciesStore;
    this.speciesStoreSupplier = speciesStoreSupplier;
    this.dataResourceStore = dataResourceStore;
    this.dataResourceStoreSupplier = dataResourceStoreSupplier;
    this.conservationService = conservationService;
    this.conservationServiceSupplier = conservationServiceSupplier;
    this.erTag = erTag;
    this.trTag = trTag;
    this.lrTag = lrTag;
    this.txrTag = txrTag;
    this.atxrTag = atxrTag;
  }

  /**
   * Maps {@link ALASensitivityRecord} to key value, where key is {@link ALASensitivityRecord#getId}
   */
  public MapElements<ALASensitivityRecord, KV<String, ALASensitivityRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ALASensitivityRecord>>() {})
        .via((ALASensitivityRecord tr) -> KV.of(tr.getId(), tr));
  }

  public ALASensitiveDataTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  public ALASensitiveDataTransform init() {
    setup();
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (this.speciesStore == null && this.speciesStoreSupplier != null) {
      log.info("Initialize SensitiveSpeciesKvStore");
      this.speciesStore = this.speciesStoreSupplier.get();
    }
    if (this.dataResourceStore == null && this.dataResourceStoreSupplier != null) {
      log.info("Initialize CollectoryKvStore");
      this.dataResourceStore = this.dataResourceStoreSupplier.get();
    }
    if (this.conservationService == null && this.conservationServiceSupplier != null) {
      log.info("Initialize Conservation API");
      this.conservationService = this.conservationServiceSupplier.get();
    }
    if (this.sensitiveFields == null) {
      this.sensitiveFields =
          SensitiveDataInterpreter.buildSensitivityMap(
              this.conservationService.getSensitiveDataFields());
    }
  }

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {
    // This section if uncommented cause CacheClosedExceptions
    // to be thrown by the transform due to its use
    // of the dataResourceStore
  }

  /**
   * Gather the information from the various interpretations of the data and provide a sensitivity
   * statement, describing what aspects are to be genealised.
   *
   * @param source The collection of tuples that make up the data
   * @return The sensitivity record for this occurrence record
   */
  @Override
  public Optional<ALASensitivityRecord> convert(KV<String, CoGbkResult> source) {

    CoGbkResult v = source.getValue();
    String id = source.getKey();

    if (v == null) return Optional.empty();

    ExtendedRecord ier = this.erTag == null ? null : v.getOnly(this.erTag, null);
    TemporalRecord itr = this.trTag == null ? null : v.getOnly(this.trTag, null);
    TaxonRecord itxr = this.txrTag == null ? null : v.getOnly(this.txrTag, null);
    ALATaxonRecord iatxr = this.atxrTag == null ? null : v.getOnly(this.atxrTag, null);
    LocationRecord ilr = this.lrTag == null ? null : v.getOnly(lrTag, null);
    ALASensitivityRecord sr = ALASensitivityRecord.newBuilder().setId(id).build();
    ALACollectoryMetadata dataResource =
        dataResourceStore == null ? null : dataResourceStore.get(datasetId);

    Map<String, String> properties = new HashMap<>(this.sensitiveFields.size());
    properties.put(
        DwcTerm.scientificName.simpleName(),
        SensitiveDataInterpreter.extractScientificName(iatxr, itxr, ier));
    properties.put(
        DwcTerm.taxonConceptID.simpleName(),
        SensitiveDataInterpreter.extractTaxonId(iatxr, itxr, ier));
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, iatxr);
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, itxr);
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, itr);
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, ilr);
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, ier);

    if (SensitiveDataInterpreter.sourceQualityChecks(properties, sr, dataResource)) {
      SensitiveDataInterpreter.sensitiveDataInterpreter(
          dataResource, speciesStore, conservationService, properties, sr);
    }
    return Optional.of(sr);
  }

  /**
   * Rewrite a record with sensitive generalisations.
   *
   * @param clazz The class of record being processed
   * @param ctag The tuple tag to use for retrieval
   * @param <T> The type of record being re-written
   * @return A Singleton ParDo for the rewriter
   */
  public <T extends SpecificRecordBase> ParDo.SingleOutput<KV<String, CoGbkResult>, T> rewriter(
      Class<T> clazz, TupleTag<T> ctag) {
    DoFn<KV<String, CoGbkResult>, T> fn =
        new DoFn<KV<String, CoGbkResult>, T>() {

          private final Counter counter =
              Metrics.counter(
                  ALASensitiveDataTransform.class, "generalise" + clazz.getSimpleName() + "Count");

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();

            T record = v.getOnly(ctag, null);
            T generalised = null;
            if (record == null) return;
            ALASensitiveDataTransform.this.setup(); // Required to ensure sensitive fields present
            ALASensitivityRecord sr = v.getOnly(ALASensitiveDataTransform.this.getTag(), null);
            if (sr == null || sr.getSensitive() == null || !sr.getSensitive()) {
              generalised = record;
            } else {
              if (record instanceof ExtendedRecord) {
                ExtendedRecord r = ExtendedRecord.newBuilder((ExtendedRecord) record).build();
                SensitiveDataInterpreter.applySensitivity(sensitiveFields, sr, r);
                generalised = (T) r;
              } else if (record instanceof TemporalRecord) {
                TemporalRecord r = TemporalRecord.newBuilder((TemporalRecord) record).build();
                SensitiveDataInterpreter.applySensitivity(sensitiveFields, sr, r);
                generalised = (T) r;
              } else if (record instanceof LocationRecord) {
                LocationRecord r = LocationRecord.newBuilder((LocationRecord) record).build();
                SensitiveDataInterpreter.applySensitivity(sensitiveFields, sr, r);
                generalised = (T) r;
              } else if (record instanceof TaxonRecord) {
                TaxonRecord r = TaxonRecord.newBuilder((TaxonRecord) record).build();
                SensitiveDataInterpreter.applySensitivity(sensitiveFields, sr, r);
                generalised = (T) r;
              } else if (record instanceof ALATaxonRecord) {
                ALATaxonRecord r = ALATaxonRecord.newBuilder((ALATaxonRecord) record).build();
                SensitiveDataInterpreter.applySensitivity(sensitiveFields, sr, r);
                generalised = (T) r;
              } else {
                log.error("Unable to process sensitive record of class " + record.getClass());
              }
            }
            if (generalised != null) {
              c.output(generalised);
              counter.inc();
            }
          }
        };
    return ParDo.of(fn);
  }
}
