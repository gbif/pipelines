package au.org.ala.pipelines.transforms;

import static au.org.ala.pipelines.common.ALARecordTypes.ALA_SENSITIVE_DATA;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.pipelines.interpreters.SensitiveDataInterpreter;
import au.org.ala.pipelines.vocabulary.Sensitivity;
import au.org.ala.pipelines.vocabulary.Vocab;
import au.org.ala.sds.api.ConservationApi;
import au.org.ala.sds.api.SensitivityQuery;
import au.org.ala.sds.api.SensitivityReport;
import au.org.ala.sds.api.SpeciesCheck;
import au.org.ala.sds.generalise.FieldAccessor;
import au.org.ala.sds.generalise.Generalisation;
import java.io.FileNotFoundException;
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
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.*;
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

  /** Fields that indicate that the raw 2record has been generalised */
  private static final Set<Term> GENERALISATION_FIELDS =
      new HashSet<>(Arrays.asList(DwcTerm.dataGeneralizations, DwcTerm.informationWithheld));

  private final ALAPipelinesConfig config;
  private final String datasetId;
  private KeyValueStore<SpeciesCheck, Boolean> speciesStore;
  private KeyValueStore<SensitivityQuery, SensitivityReport> reportStore;
  private ConservationApi conservationService;
  private final SerializableSupplier<KeyValueStore<SpeciesCheck, Boolean>> speciesStoreSupplier;
  private final SerializableSupplier<KeyValueStore<SensitivityQuery, SensitivityReport>>
      reportStoreSupplier;
  private final SerializableSupplier<ConservationApi> conservationServiceSupplier;
  private Vocab sensitivityVocab;

  private List<Generalisation> generalisations;
  private Set<Term> sensitiveFields;

  @NonNull private final TupleTag<ExtendedRecord> erTag;
  @NonNull private final TupleTag<TemporalRecord> trTag;
  @NonNull private final TupleTag<TaxonRecord> txrTag;
  @NonNull private final TupleTag<ALATaxonRecord> atxrTag;
  @NonNull private final TupleTag<LocationRecord> lrTag;

  @Builder(buildMethodName = "create")
  private ALASensitiveDataRecordTransform(
      ALAPipelinesConfig config,
      String datasetId,
      KeyValueStore<SpeciesCheck, Boolean> speciesStore,
      SerializableSupplier<KeyValueStore<SpeciesCheck, Boolean>> speciesStoreSupplier,
      KeyValueStore<SensitivityQuery, SensitivityReport> reportStore,
      SerializableSupplier<KeyValueStore<SensitivityQuery, SensitivityReport>> reportStoreSupplier,
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

    this.config = config;
    this.datasetId = datasetId;
    this.speciesStore = speciesStore;
    this.reportStore = reportStore;

    this.speciesStoreSupplier = speciesStoreSupplier;
    this.reportStoreSupplier = reportStoreSupplier;
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
    if (speciesStore == null && speciesStoreSupplier != null) {
      log.debug("Initialize Sensitive Species KvStore");
      speciesStore = speciesStoreSupplier.get();
    }
    if (reportStore == null && reportStoreSupplier != null) {
      log.debug("Initialize Sensitivity Report KvStore");
      reportStore = reportStoreSupplier.get();
    }
    if (conservationService == null && conservationServiceSupplier != null) {
      log.debug("Initialize Conservation API");
      conservationService = conservationServiceSupplier.get();
    }
    if (generalisations == null && conservationService != null) {
      log.debug("Get generalisations to apply");
      generalisations = conservationService.getGeneralisations();
      sensitiveFields = null;
    }
    if (sensitiveFields == null && generalisations != null) {
      log.debug("Building sensitive field list");
      sensitiveFields = new HashSet<>(generalisations.size());
      for (Generalisation g : generalisations) {
        sensitiveFields.addAll(
            g.getFields().stream().map(FieldAccessor::getField).collect(Collectors.toSet()));
      }
    }
    try {
      sensitivityVocab =
          Sensitivity.getInstance(config != null ? config.getSensitivityVocabFile() : null);
    } catch (FileNotFoundException e) {
      log.error("Unable to get vocabulary file " + config.getSensitivityVocabFile());
      sensitivityVocab = null;
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

    if (v == null) {
      return Optional.empty();
    }

    ExtendedRecord ier = erTag == null ? null : v.getOnly(erTag, null);
    TemporalRecord itr = trTag == null ? null : v.getOnly(trTag, null);
    TaxonRecord itxr = txrTag == null ? null : v.getOnly(txrTag, null);
    ALATaxonRecord iatxr = atxrTag == null ? null : v.getOnly(atxrTag, null);
    LocationRecord ilr = lrTag == null ? null : v.getOnly(lrTag, null);

    ALASensitivityRecord sr = ALASensitivityRecord.newBuilder().setId(id).build();

    Map<String, String> properties = new HashMap<>(sensitiveFields.size());
    properties.put(
        DwcTerm.scientificName.qualifiedName(),
        SensitiveDataInterpreter.extractScientificName(iatxr, itxr, ier));
    properties.put(
        DwcTerm.taxonConceptID.qualifiedName(),
        SensitiveDataInterpreter.extractTaxonId(iatxr, itxr, ier));

    Map<String, String> dataGeneralisations = new HashMap<>();
    SensitiveDataInterpreter.constructFields(GENERALISATION_FIELDS, dataGeneralisations, ier);

    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, iatxr);
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, itxr);
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, itr);
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, ilr);
    SensitiveDataInterpreter.constructFields(sensitiveFields, properties, ier);

    if (SensitiveDataInterpreter.sourceQualityChecks(properties, sr)) {
      SensitiveDataInterpreter.sensitiveDataInterpreter(
          speciesStore,
          reportStore,
          generalisations,
          datasetId,
          properties,
          dataGeneralisations,
          sensitivityVocab,
          sr);
    }
    return Optional.of(sr);
  }
}
