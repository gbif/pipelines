package au.org.ala.pipelines.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;

import au.org.ala.pipelines.converters.ALAOccurrenceJsonConverter;
import au.org.ala.pipelines.converters.ALAParentJsonConverter;
import java.io.Serializable;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;

/**
 * Beam level transformation for the ES output json. The transformation consumes objects, which
 * classes were generated from avro schema files and converts into json string object
 *
 * <p>Example:
 *
 * <p>
 *
 * <pre>{@code
 * final TupleTag<ExtendedRecord> erTag = new TupleTag<ExtendedRecord>() {};
 * final TupleTag<BasicRecord> brTag = new TupleTag<BasicRecord>() {};
 * final TupleTag<TemporalRecord> trTag = new TupleTag<TemporalRecord>() {};
 * final TupleTag<LocationRecord> lrTag = new TupleTag<LocationRecord>() {};
 * final TupleTag<TaxonRecord> txrTag = new TupleTag<TaxonRecord>() {};
 * final TupleTag<MultimediaRecord> mrTag = new TupleTag<MultimediaRecord>() {};
 * final TupleTag<ImageRecord> irTag = new TupleTag<ImageRecord>() {};
 * final TupleTag<AudubonRecord> arTag = new TupleTag<AudubonRecord>() {};
 * final TupleTag<MeasurementOrFactRecord> mfrTag = new TupleTag<MeasurementOrFactRecord>() {};
 *
 * PCollectionView<MetadataRecord> metadataView = ...
 * PCollection<KV<String, ExtendedRecord>> verbatimCollection = ...
 * PCollection<KV<String, BasicRecord>> basicCollection = ...
 * PCollection<KV<String, TemporalRecord>> temporalCollection = ...
 * PCollection<KV<String, LocationRecord>> locationCollection = ...
 * PCollection<KV<String, TaxonRecord>> taxonCollection = ...
 * PCollection<KV<String, MultimediaRecord>> multimediaCollection = ...
 * PCollection<KV<String, ImageRecord>> imageCollection = ...
 * PCollection<KV<String, AudubonRecord>> audubonCollection = ...
 * PCollection<KV<String, MeasurementOrFactRecord>> measurementCollection = ...
 *
 * SingleOutput<KV<String, CoGbkResult>, String> occurrenceJsonDoFn =
 *     OccurrenceJsonTransform.create(erTag, brTag, trTag, lrTag, txrTag, mrTag, irTag, arTag, mfrTag, metadataView)
 *         .converter();
 *
 * PCollection<String> jsonCollection =
 *     KeyedPCollectionTuple
 *         // Core
 *         .of(brTag, basicCollection)
 *         .and(trTag, temporalCollection)
 *         .and(lrTag, locationCollection)
 *         .and(txrTag, taxonCollection)
 *         // Extension
 *         .and(mrTag, multimediaCollection)
 *         .and(irTag, imageCollection)
 *         .and(arTag, audubonCollection)
 *         .and(mfrTag, measurementCollection)
 *         // Raw
 *         .and(erTag, verbatimCollection)
 *         // Apply
 *         .apply("Grouping objects", CoGroupByKey.create())
 *         .apply("Merging to json", occurrenceJsonDoFn);
 * }</pre>
 */
@SuppressWarnings("ConstantConditions")
@Builder
public class ALAOccurrenceJsonTransform implements Serializable {

  private static final long serialVersionUID = 1279313931024806171L;

  // Core
  @NonNull private final TupleTag<ALAUUIDRecord> uuidRecordTag;
  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;
  @NonNull private final TupleTag<BasicRecord> basicRecordTag;
  @NonNull private final TupleTag<TemporalRecord> temporalRecordTag;
  @NonNull private final TupleTag<LocationRecord> locationRecordTag;
  @NonNull private final TupleTag<ALATaxonRecord> taxonRecordTag;
  @NonNull private final TupleTag<MultimediaRecord> multimediaRecordTag;
  @NonNull private final TupleTag<EventCoreRecord> eventCoreRecordTag;
  @NonNull private final PCollectionView<ALAMetadataRecord> metadataView;
  @NonNull private final TupleTag<MeasurementOrFactRecord> measurementOrFactRecordTupleTag;

  @NonNull private final TupleTag<LocationInheritedRecord> locationInheritedRecordTag;
  @NonNull private final TupleTag<TemporalInheritedRecord> temporalInheritedRecordTag;
  @NonNull private final TupleTag<EventInheritedRecord> eventInheritedRecordTag;
  @NonNull private final TupleTag<ALASensitivityRecord> sensitivityRecordTag;

  // Determines if the output record is a parent-child record
  @Builder.Default private final boolean asParentChildRecord = false;

  public SingleOutput<KV<String, CoGbkResult>, String> converter() {

    DoFn<KV<String, CoGbkResult>, String> fn =
        new DoFn<KV<String, CoGbkResult>, String>() {

          private final Counter counter =
              Metrics.counter(ALAOccurrenceJsonTransform.class, AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // Core
            ALAMetadataRecord mdr = c.sideInput(metadataView);
            ALAUUIDRecord uuidr =
                v.getOnly(uuidRecordTag, ALAUUIDRecord.newBuilder().setId(k).build());

            ExtendedRecord er =
                v.getOnly(extendedRecordTag, ExtendedRecord.newBuilder().setId(k).build());
            BasicRecord br = v.getOnly(basicRecordTag, BasicRecord.newBuilder().setId(k).build());
            TemporalRecord tr =
                v.getOnly(temporalRecordTag, TemporalRecord.newBuilder().setId(k).build());
            LocationRecord lr =
                v.getOnly(locationRecordTag, LocationRecord.newBuilder().setId(k).build());
            ALATaxonRecord txr =
                v.getOnly(taxonRecordTag, ALATaxonRecord.newBuilder().setId(k).build());
            MeasurementOrFactRecord mfr =
                v.getOnly(
                    measurementOrFactRecordTupleTag,
                    MeasurementOrFactRecord.newBuilder().setId(k).build());
            EventCoreRecord ecr =
                v.getOnly(eventCoreRecordTag, EventCoreRecord.newBuilder().setId(k).build());

            // Inherited
            EventInheritedRecord eir =
                v.getOnly(
                    eventInheritedRecordTag, EventInheritedRecord.newBuilder().setId(k).build());
            LocationInheritedRecord lir =
                v.getOnly(
                    locationInheritedRecordTag,
                    LocationInheritedRecord.newBuilder().setId(k).build());
            TemporalInheritedRecord tir =
                v.getOnly(
                    temporalInheritedRecordTag,
                    TemporalInheritedRecord.newBuilder().setId(k).build());

            ALASensitivityRecord sr =
                v.getOnly(sensitivityRecordTag, ALASensitivityRecord.newBuilder().setId(k).build());

            String json = null;

            // if a record is deemed sensitive and is associated with an event
            // block indexing
            if (!isSensitive(ecr, sr)) {

              ALAOccurrenceJsonConverter occurrenceJsonConverter =
                  ALAOccurrenceJsonConverter.builder()
                      .uuid(uuidr)
                      .metadata(mdr)
                      .basic(br)
                      .temporal(tr)
                      .location(lr)
                      .taxon(txr)
                      .verbatim(er)
                      .measurementOrFact(mfr)
                      .eventCore(ecr)
                      .eventInheritedRecord(eir)
                      .locationInheritedRecord(lir)
                      .temporalInheritedRecord(tir)
                      .sensitivityRecord(sr)
                      .build();
              if (asParentChildRecord) {
                json =
                    ALAParentJsonConverter.builder()
                        .occurrenceJsonRecord(occurrenceJsonConverter.convert())
                        .metadata(mdr)
                        .uuid(uuidr)
                        .eventCore(ecr)
                        .temporal(tr)
                        .location(lr)
                        .verbatim(er)
                        .eventInheritedRecord(eir)
                        .locationInheritedRecord(lir)
                        .temporalInheritedRecord(tir)
                        .build()
                        .toJson();
              } else {
                json = occurrenceJsonConverter.toJson();
              }

              if (json != null) {
                c.output(json);
                counter.inc();
              }
            }
          }
        };

    return ParDo.of(fn).withSideInputs(metadataView);
  }

  private boolean isSensitive(EventCoreRecord ecr, ALASensitivityRecord sr) {
    return sr != null
        && (sr.getIsSensitive() != null && sr.getIsSensitive() && ecr.getId() != null);
  }
}
