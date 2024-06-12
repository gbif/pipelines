package uk.org.nbn.pipelines.transforms;

import static uk.org.nbn.pipelines.common.NBNRecordTypes.NBN_ACCESS_CONTROLLED_DATA;

import au.org.ala.kvs.ALAPipelinesConfig;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
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
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.transforms.Transform;
import uk.org.nbn.pipelines.interpreters.NBNAccessControlledDataInterpreter;
import uk.org.nbn.pipelines.io.avro.NBNAccessControlledRecord;

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
public class NBNAccessControlRecordTransform
    extends Transform<KV<String, CoGbkResult>, NBNAccessControlledRecord> {

  /** Fields that indicate that the raw 2record has been generalised */
  private static final Set<Term> GENERALISATION_FIELDS =
      new HashSet<>(Arrays.asList(DwcTerm.dataGeneralizations, DwcTerm.informationWithheld));

  private final ALAPipelinesConfig config;
  private final String datasetId;

  @NonNull private final TupleTag<ExtendedRecord> erTag;
  @NonNull private final TupleTag<LocationRecord> lrTag;

  @Builder(buildMethodName = "create")
  private NBNAccessControlRecordTransform(
      ALAPipelinesConfig config,
      String datasetId,
      TupleTag<ExtendedRecord> erTag,
      TupleTag<LocationRecord> lrTag) {
    super(
        NBNAccessControlledRecord.class,
        NBN_ACCESS_CONTROLLED_DATA,
        NBNAccessControlRecordTransform.class.getName(),
        "nbnAccessControlledDataRecordCount");

    this.config = config;
    this.datasetId = datasetId;
    this.erTag = erTag;
    this.lrTag = lrTag;
  }

  /**
   * Maps {@link NBNAccessControlledRecord} to key value, where key is {@link
   * NBNAccessControlledRecord#getId}
   */
  public MapElements<NBNAccessControlledRecord, KV<String, NBNAccessControlledRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, NBNAccessControlledRecord>>() {})
        .via((NBNAccessControlledRecord tr) -> KV.of(tr.getId(), tr));
  }

  public NBNAccessControlRecordTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  public NBNAccessControlRecordTransform init() {
    setup();
    return this;
  }

  /** Beam @Setup initializes resources */
  @Setup
  public void setup() {}

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
  public Optional<NBNAccessControlledRecord> convert(KV<String, CoGbkResult> source) {

    CoGbkResult v = source.getValue();
    String id = source.getKey();

    if (v == null) {
      return Optional.empty();
    }

    LocationRecord lr = lrTag == null ? null : v.getOnly(lrTag, null);
    ExtendedRecord er = erTag == null ? null : v.getOnly(erTag, null);
    // TODO HMJ OSGridRecord osgr = osgTag == null ? null : v.getOnly(osgTag, null);

    NBNAccessControlledRecord accessControlledRecord =
        NBNAccessControlledRecord.newBuilder().setId(id).build();

    NBNAccessControlledDataInterpreter.accessControlledDataInterpreter(
        datasetId,
        er,
        lr,
        // TODO HMJ     osgr,
        accessControlledRecord);

    return Optional.of(accessControlledRecord);
  }
}
