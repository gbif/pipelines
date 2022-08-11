package org.gbif.pipelines.transforms.converters;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EVENTS_AVRO_TO_JSON_COUNT;

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
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.core.converters.specific.GbifParentJsonConverter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
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
 * final TupleTag<IdentifierRecord> irTag = new TupleTag<IdentifierRecord>() {};
 * final TupleTag<EventCoreRecord> ecrTag = new TupleTag<EventCoreRecord>() {};
 *
 * PCollection<KV<String, ExtendedRecord>> verbatimCollection = ...
 * PCollection<KV<String, EventCoreRecord>> eventCoreRecordCollection = ...
 * PCollection<KV<String, IdentifierRecord>> identifierRecordCollection = ...
 *
 * SingleOutput<KV<String, CoGbkResult>, String> eventJsonDoFn =
 * EventCoreJsonTransform.builder()
 *    .extendedRecordTag(verbatimTransform.getTag())
 *    .identifierRecordTag(identifierTransform.getTag())
 *    .eventCoreRecordTag(eventCoreTransform.getTag())
 *    .build()
 *    .converter();
 *
 * PCollection<String> jsonCollection =
 *    KeyedPCollectionTuple
 *    // Core
 *    .of(eventCoreTransform.getTag(), eventCoreCollection)
 *    // Internal
 *    .and(identifierTransform.getTag(), identifierCollection)
 *    // Raw
 *    .and(verbatimTransform.getTag(), verbatimCollection)
 *    // Apply
 *    .apply("Grouping objects", CoGroupByKey.create())
 *    .apply("Merging to json", eventJsonDoFn);
 * }</pre>
 */
@SuppressWarnings("ConstantConditions")
@Builder
public class ParentJsonTransform implements Serializable {

  private static final long serialVersionUID = 1279313941024805871L;

  // Core
  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;
  @NonNull private final TupleTag<EventCoreRecord> eventCoreRecordTag;
  @NonNull private final TupleTag<IdentifierRecord> identifierRecordTag;
  @NonNull private final TupleTag<TemporalRecord> temporalRecordTag;
  @NonNull private final TupleTag<LocationRecord> locationRecordTag;
  @NonNull private final TupleTag<TaxonRecord> taxonRecordTag;
  // Extension
  @NonNull private final TupleTag<MultimediaRecord> multimediaRecordTag;
  @NonNull private final TupleTag<ImageRecord> imageRecordTag;
  @NonNull private final TupleTag<AudubonRecord> audubonRecordTag;
  @NonNull private final PCollectionView<MetadataRecord> metadataView;
  @NonNull private final TupleTag<DerivedMetadataRecord> derivedMetadataRecordTag;
  @NonNull private final TupleTag<MeasurementOrFactRecord> measurementOrFactRecordTag;

  @NonNull private final TupleTag<LocationInheritedRecord> locationInheritedRecordTag;
  @NonNull private final TupleTag<TemporalInheritedRecord> temporalInheritedRecordTag;
  @NonNull private final TupleTag<EventInheritedRecord> eventInheritedRecordTag;

  public SingleOutput<KV<String, CoGbkResult>, String> converter() {

    DoFn<KV<String, CoGbkResult>, String> fn =
        new DoFn<KV<String, CoGbkResult>, String>() {

          private final Counter counter =
              Metrics.counter(ParentJsonTransform.class, EVENTS_AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // Core
            MetadataRecord mdr = c.sideInput(metadataView);
            ExtendedRecord er =
                v.getOnly(extendedRecordTag, ExtendedRecord.newBuilder().setId(k).build());
            EventCoreRecord ecr =
                v.getOnly(eventCoreRecordTag, EventCoreRecord.newBuilder().setId(k).build());
            IdentifierRecord ir =
                v.getOnly(identifierRecordTag, IdentifierRecord.newBuilder().setId(k).build());
            TemporalRecord tr =
                v.getOnly(temporalRecordTag, TemporalRecord.newBuilder().setId(k).build());
            LocationRecord lr =
                v.getOnly(locationRecordTag, LocationRecord.newBuilder().setId(k).build());
            TaxonRecord txr = v.getOnly(taxonRecordTag, TaxonRecord.newBuilder().setId(k).build());

            // Extension
            MultimediaRecord mr =
                v.getOnly(multimediaRecordTag, MultimediaRecord.newBuilder().setId(k).build());
            ImageRecord imr = v.getOnly(imageRecordTag, ImageRecord.newBuilder().setId(k).build());
            AudubonRecord ar =
                v.getOnly(audubonRecordTag, AudubonRecord.newBuilder().setId(k).build());
            MeasurementOrFactRecord mofr =
                v.getOnly(
                    measurementOrFactRecordTag,
                    MeasurementOrFactRecord.newBuilder().setId(k).build());

            MultimediaRecord mmr = MultimediaConverter.merge(mr, imr, ar);

            // Derived metadata
            DerivedMetadataRecord dmr =
                v.getOnly(
                    derivedMetadataRecordTag, DerivedMetadataRecord.newBuilder().setId(k).build());

            // Inherited location fields
            LocationInheritedRecord lir =
                v.getOnly(
                    locationInheritedRecordTag,
                    LocationInheritedRecord.newBuilder().setId(k).build());

            // Inherited temporal fields
            TemporalInheritedRecord tir =
                v.getOnly(
                    temporalInheritedRecordTag,
                    TemporalInheritedRecord.newBuilder().setId(k).build());

            // Inherited temporal fields
            EventInheritedRecord eir =
                v.getOnly(
                    eventInheritedRecordTag, EventInheritedRecord.newBuilder().setId(k).build());

            // Convert and
            String json =
                GbifParentJsonConverter.builder()
                    .metadata(mdr)
                    .eventCore(ecr)
                    .identifier(ir)
                    .temporal(tr)
                    .location(lr)
                    .multimedia(mmr)
                    .verbatim(er)
                    .taxon(txr)
                    .derivedMetadata(dmr)
                    .locationInheritedRecord(lir)
                    .measurementOrFactRecord(mofr)
                    .temporalInheritedRecord(tir)
                    .eventInheritedRecord(eir)
                    .build()
                    .toJson();

            c.output(json);
            counter.inc();
          }
        };

    return ParDo.of(fn).withSideInputs(metadataView);
  }
}
