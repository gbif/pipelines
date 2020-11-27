package au.org.ala.pipelines.transforms;

import au.org.ala.pipelines.interpreters.SensitiveDataInterpreter;
import au.org.ala.sds.api.ConservationApi;
import au.org.ala.sds.generalise.FieldAccessor;
import au.org.ala.sds.generalise.Generalisation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.data.RecordBuilder;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.common.PipelinesVariables;
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
public class ALASensitiveDataApplicationTransform<T extends SpecificRecordBase>
    extends Transform<KV<String, CoGbkResult>, T> {

  private ConservationApi conservationService;
  private final SerializableSupplier<ConservationApi> conservationServiceSupplier;
  private List<Generalisation> generalisations;
  private Set<Term> sensitiveFields;
  private TupleTag<T> rTag;
  private TupleTag<ALASensitivityRecord> srTag;
  private Method builder;

  public ALASensitiveDataApplicationTransform(
      Class<T> clazz,
      PipelinesVariables.Pipeline.Interpretation.InterpretationType type,
      ConservationApi conservationService,
      SerializableSupplier<ConservationApi> conservationServiceSupplier,
      TupleTag<T> rTag,
      TupleTag<ALASensitivityRecord> srTag)
      throws IllegalArgumentException {
    super(
        clazz,
        type,
        ALASensitiveDataApplicationTransform.class.getName(),
        "alaSensitiveDataApplication" + "_" + clazz.getSimpleName() + "_Count");
    this.conservationService = conservationService;
    this.conservationServiceSupplier = conservationServiceSupplier;
    this.rTag = rTag;
    this.srTag = srTag;
  }

  /**
   * Maps {@link ALASensitivityRecord} to key value, where key is {@link ALASensitivityRecord#getId}
   */
  public MapElements<ALASensitivityRecord, KV<String, ALASensitivityRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ALASensitivityRecord>>() {})
        .via((ALASensitivityRecord tr) -> KV.of(tr.getId(), tr));
  }

  public ALASensitiveDataApplicationTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  public ALASensitiveDataApplicationTransform init() {
    setup();
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {
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
    if (this.builder == null) {
      Class<T> clazz = this.getClazz();
      try {
        this.builder = clazz.getMethod("newBuilder", clazz);
      } catch (NoSuchMethodException ex) {
        throw new IllegalArgumentException("Class " + clazz + " requires newBuilder method", ex);
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
  public Optional<T> convert(KV<String, CoGbkResult> source) {
    CoGbkResult v = source.getValue();
    String id = source.getKey();

    if (v == null) return Optional.empty();

    T record = v.getOnly(this.rTag, null);
    T generalised = null;
    if (record == null) return Optional.empty();
    ALASensitivityRecord sr = v.getOnly(this.srTag, null);
    if (sr == null || sr.getSensitive() == null || !sr.getSensitive()) {
      generalised = record;
    } else {
      try {
        generalised = ((RecordBuilder<T>) this.builder.invoke(null, record)).build();
      } catch (Exception ex) {
        log.error("Unable to construct copy of incoming record " + record, ex);
        return Optional.empty();
      }
      if (record instanceof ExtendedRecord) {
        SensitiveDataInterpreter.applySensitivity(
            sensitiveFields, sr, (ExtendedRecord) generalised);
      } else if (record instanceof TemporalRecord) {
        SensitiveDataInterpreter.applySensitivity(
            sensitiveFields, sr, (TemporalRecord) generalised);
      } else if (record instanceof LocationRecord) {
        SensitiveDataInterpreter.applySensitivity(
            sensitiveFields, sr, (LocationRecord) generalised);
      } else if (record instanceof TaxonRecord) {
        SensitiveDataInterpreter.applySensitivity(sensitiveFields, sr, (TaxonRecord) generalised);
      } else if (record instanceof ALATaxonRecord) {
        SensitiveDataInterpreter.applySensitivity(
            sensitiveFields, sr, (ALATaxonRecord) generalised);
      } else {
        log.error("Unable to process sensitive record of class " + record.getClass());
      }
    }
    return Optional.ofNullable(generalised);
  }
}
