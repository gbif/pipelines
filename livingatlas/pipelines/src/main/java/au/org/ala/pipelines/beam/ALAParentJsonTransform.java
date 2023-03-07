package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.EVENTS_AVRO_TO_JSON_COUNT;

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
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;

/** ALA version of ParentJsonTransform */
@SuppressWarnings("ConstantConditions")
@Builder
public class ALAParentJsonTransform implements Serializable {

  private static final long serialVersionUID = 1279313941024805871L;

  // Core
  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;
  @NonNull private final TupleTag<EventCoreRecord> eventCoreRecordTag;
  @NonNull private final TupleTag<IdentifierRecord> identifierRecordTag;
  @NonNull private final TupleTag<TemporalRecord> temporalRecordTag;
  @NonNull private final TupleTag<LocationRecord> locationRecordTag;
  private final TupleTag<TaxonRecord> taxonRecordTag;
  // Extension
  @NonNull private final TupleTag<MultimediaRecord> multimediaRecordTag;
  @NonNull private final TupleTag<ImageRecord> imageRecordTag;
  @NonNull private final TupleTag<AudubonRecord> audubonRecordTag;
  @NonNull private final PCollectionView<ALAMetadataRecord> metadataView;
  @NonNull private final TupleTag<DerivedMetadataRecord> derivedMetadataRecordTag;
  @NonNull private final TupleTag<MeasurementOrFactRecord> measurementOrFactRecordTag;
  private final TupleTag<String[]> samplingProtocolsTag;

  @NonNull private final TupleTag<LocationInheritedRecord> locationInheritedRecordTag;
  @NonNull private final TupleTag<TemporalInheritedRecord> temporalInheritedRecordTag;
  @NonNull private final TupleTag<EventInheritedRecord> eventInheritedRecordTag;

  public SingleOutput<KV<String, CoGbkResult>, String> converter() {

    DoFn<KV<String, CoGbkResult>, String> fn =
        new DoFn<KV<String, CoGbkResult>, String>() {

          private final Counter counter =
              Metrics.counter(ALAParentJsonTransform.class, EVENTS_AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // Core
            ALAMetadataRecord mdr = c.sideInput(metadataView);
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

            // Extension
            MultimediaRecord mr =
                v.getOnly(multimediaRecordTag, MultimediaRecord.newBuilder().setId(k).build());
            ImageRecord imr = v.getOnly(imageRecordTag, ImageRecord.newBuilder().setId(k).build());
            AudubonRecord ar =
                v.getOnly(audubonRecordTag, AudubonRecord.newBuilder().setId(k).build());

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

            MeasurementOrFactRecord mofr =
                v.getOnly(
                    measurementOrFactRecordTag,
                    MeasurementOrFactRecord.newBuilder().setId(k).build());

            MultimediaRecord mmr = MultimediaConverter.merge(mr, imr, ar);

            // Derived metadata
            DerivedMetadataRecord dmr =
                v.getOnly(
                    derivedMetadataRecordTag, DerivedMetadataRecord.newBuilder().setId(k).build());

            // Convert to JSON
            String json =
                ALAParentJsonConverter.builder()
                    .metadata(mdr)
                    .eventCore(ecr)
                    .identifier(ir)
                    .temporal(tr)
                    .location(lr)
                    .multimedia(mmr)
                    .verbatim(er)
                    .derivedMetadata(dmr)
                    .measurementOrFactRecord(mofr)
                    .build()
                    .toJson();

            c.output(json);
            counter.inc();
          }
        };

    return ParDo.of(fn).withSideInputs(metadataView);
  }
}
