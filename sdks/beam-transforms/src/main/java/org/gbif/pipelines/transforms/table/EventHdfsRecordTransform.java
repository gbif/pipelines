package org.gbif.pipelines.transforms.table;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_HDFS_COUNT;

import java.io.Serializable;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.converters.EventHdfsRecordConverter;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.HumboldtRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.event.EventHdfsRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformation for Occurrence HDFS Downloads Table. The transformation consumes
 * objects, which classes were generated from avro schema files and converts into json string object
 */
@SuppressWarnings("ConstantConditions")
@Builder
public class EventHdfsRecordTransform implements Serializable {

  private static final long serialVersionUID = 4605359346756029672L;

  // Core
  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;

  @NonNull private final TupleTag<IdentifierRecord> identifierRecordTag;
  @NonNull private final TupleTag<TemporalRecord> temporalRecordTag;
  @NonNull private final TupleTag<LocationRecord> locationRecordTag;
  @NonNull private final TupleTag<EventCoreRecord> eventCoreRecordTag;

  // Extension
  @NonNull private final TupleTag<MultimediaRecord> multimediaRecordTag;
  @NonNull private final TupleTag<ImageRecord> imageRecordTag;
  @NonNull private final TupleTag<AudubonRecord> audubonRecordTag;
  @NonNull private final TupleTag<HumboldtRecord> humboldtRecordTag;

  @NonNull private final PCollectionView<MetadataRecord> metadataView;

  public SingleOutput<KV<String, CoGbkResult>, EventHdfsRecord> converter() {

    DoFn<KV<String, CoGbkResult>, EventHdfsRecord> fn =
        new DoFn<KV<String, CoGbkResult>, EventHdfsRecord>() {

          private final Counter counter =
              Metrics.counter(EventHdfsRecordTransform.class, AVRO_TO_HDFS_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // Core
            MetadataRecord mdr = c.sideInput(metadataView);
            IdentifierRecord id = v.getOnly(identifierRecordTag);
            ExtendedRecord er =
                v.getOnly(extendedRecordTag, ExtendedRecord.newBuilder().setId(k).build());
            TemporalRecord tr =
                v.getOnly(temporalRecordTag, TemporalRecord.newBuilder().setId(k).build());
            LocationRecord lr =
                v.getOnly(locationRecordTag, LocationRecord.newBuilder().setId(k).build());
            // Extension
            MultimediaRecord mr =
                v.getOnly(multimediaRecordTag, MultimediaRecord.newBuilder().setId(k).build());
            ImageRecord ir = v.getOnly(imageRecordTag, ImageRecord.newBuilder().setId(k).build());
            AudubonRecord ar =
                v.getOnly(audubonRecordTag, AudubonRecord.newBuilder().setId(k).build());

            EventCoreRecord eventCoreRecord =
                v.getOnly(eventCoreRecordTag, EventCoreRecord.newBuilder().setId(k).build());
            HumboldtRecord humboldtRecord =
                v.getOnly(humboldtRecordTag, HumboldtRecord.newBuilder().setId(k).build());

            MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);

            EventHdfsRecord record =
                EventHdfsRecordConverter.builder()
                    .identifierRecord(id)
                    .metadataRecord(mdr)
                    .temporalRecord(tr)
                    .locationRecord(lr)
                    .multimediaRecord(mmr)
                    .extendedRecord(er)
                    .eventCoreRecord(eventCoreRecord)
                    .humboldtRecord(humboldtRecord)
                    .build()
                    .convert();

            c.output(record);

            counter.inc();
          }
        };

    return ParDo.of(fn).withSideInputs(metadataView);
  }

  /**
   * Writes {@link EventHdfsRecord} *.avro files to path, data will be split into several files,
   * uses Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public AvroIO.Write<EventHdfsRecord> write(String toPath, Integer numShards) {
    AvroIO.Write<EventHdfsRecord> write =
        AvroIO.write(EventHdfsRecord.class)
            .to(toPath)
            .withSuffix(PipelinesVariables.Pipeline.AVRO_EXTENSION)
            .withCodec(Transform.getBaseCodec());
    return numShards == null ? write : write.withNumShards(numShards);
  }
}
