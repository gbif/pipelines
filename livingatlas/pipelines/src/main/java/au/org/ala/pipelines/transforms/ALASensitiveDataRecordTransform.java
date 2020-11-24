package au.org.ala.pipelines.transforms;

import static au.org.ala.pipelines.common.ALARecordTypes.ALA_SENSITIVE_DATA;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.interpreters.SensitiveDataInterpreter;
import au.org.ala.sds.api.ConservationApi;
import au.org.ala.sds.api.SensitivityQuery;
import au.org.ala.sds.api.SensitivityReport;
import au.org.ala.sds.api.SpeciesCheck;
import au.org.ala.sds.generalise.FieldAccessor;
import au.org.ala.sds.generalise.Generalisation;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.transforms.SerializableConsumer;
import org.gbif.pipelines.transforms.SerializableSupplier;
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
public class ALASensitiveDataRecordTransform
    extends Transform<KV<String, CoGbkResult>, ALASensitivityRecord> {

  private final String datasetId;
  private KeyValueStore<SpeciesCheck, Boolean> speciesStore;
  private final SerializableSupplier<KeyValueStore<SpeciesCheck, Boolean>> speciesStoreSupplier;
  private KeyValueStore<SensitivityQuery, SensitivityReport> reportStore;
  private final SerializableSupplier<KeyValueStore<SensitivityQuery, SensitivityReport>>
      reportStoreSupplier;
  private KeyValueStore<String, ALACollectoryMetadata> dataResourceStore;
  private final SerializableSupplier<KeyValueStore<String, ALACollectoryMetadata>>
      dataResourceStoreSupplier;
  private ConservationApi conservationService;
  private final SerializableSupplier<ConservationApi> conservationServiceSupplier;
  private List<Generalisation> generalisations;
  private Set<Term> sensitiveFields;
  @NonNull private final TupleTag<ExtendedRecord> erTag;
  @NonNull private final TupleTag<TemporalRecord> trTag;
  @NonNull private final TupleTag<TaxonRecord> txrTag;
  @NonNull private final TupleTag<ALATaxonRecord> atxrTag;
  @NonNull private final TupleTag<LocationRecord> lrTag;

  @Builder(buildMethodName = "create")
  private ALASensitiveDataRecordTransform(
      String datasetId,
      KeyValueStore<SpeciesCheck, Boolean> speciesStore,
      SerializableSupplier<KeyValueStore<SpeciesCheck, Boolean>> speciesStoreSupplier,
      KeyValueStore<SensitivityQuery, SensitivityReport> reportStore,
      SerializableSupplier<KeyValueStore<SensitivityQuery, SensitivityReport>> reportStoreSupplier,
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
        ALASensitiveDataRecordTransform.class.getName(),
        "alaSensitiveDataRecordCount");
    this.datasetId = datasetId;
    this.speciesStore = speciesStore;
    this.speciesStoreSupplier = speciesStoreSupplier;
    this.reportStore = reportStore;
    this.reportStoreSupplier = reportStoreSupplier;
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

  public ALASensitiveDataRecordTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  public ALASensitiveDataRecordTransform init() {
    setup();
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
    if (this.speciesStore == null && this.speciesStoreSupplier != null) {
      log.info("Initialize Sensitive Species KvStore");
      this.speciesStore = this.speciesStoreSupplier.get();
    }
    if (this.reportStore == null && this.reportStoreSupplier != null) {
      log.info("Initialize Sensitivity Report KvStore");
      this.reportStore = this.reportStoreSupplier.get();
    }
    if (this.dataResourceStore == null && this.dataResourceStoreSupplier != null) {
      log.info("Initialize CollectoryKvStore");
      this.dataResourceStore = this.dataResourceStoreSupplier.get();
    }
    if (this.conservationService == null && this.conservationServiceSupplier != null) {
      log.info("Initialize Conservation API");
      this.conservationService = this.conservationServiceSupplier.get();
    }
    if (this.generalisations == null) {
      log.info("Get generalisations to apply");
      this.generalisations = this.conservationService.getGeneralisations();
      this.sensitiveFields = null;
    }
    if (this.sensitiveFields == null) {
      log.info("Building sensitive field list");
      this.sensitiveFields = new HashSet<>(this.generalisations.size());
      for (Generalisation g : this.generalisations) {
        this.sensitiveFields.addAll(
            g.getFields().stream().map(FieldAccessor::getField).collect(Collectors.toSet()));
      }
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
        DwcTerm.scientificName.qualifiedName(),
        SensitiveDataInterpreter.extractScientificName(iatxr, itxr, ier));
    properties.put(
        DwcTerm.taxonConceptID.qualifiedName(),
        SensitiveDataInterpreter.extractTaxonId(iatxr, itxr, ier));
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, iatxr);
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, itxr);
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, itr);
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, ilr);
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, ier);

    if (SensitiveDataInterpreter.sourceQualityChecks(properties, sr, dataResource)) {
      SensitiveDataInterpreter.sensitiveDataInterpreter(
          dataResource, speciesStore, reportStore, generalisations, datasetId, properties, sr);
    }
    return Optional.of(sr);
  }
}
