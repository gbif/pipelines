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
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.core.converters.OccurrenceHdfsRecordConverter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.transforms.Transform;

/**
 * Beam level transformation for Occurrence HDFS Downloads Table. The transformation consumes
 * objects, which classes were generated from avro schema files and converts into json string object
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
 * final TupleTag<GrscicollRecord> txrTag = new TupleTag<GrscicollRecord>() {};
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
 * OccurrenceHdfsRecord record = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(mdr, br, tr, lr, txr, mmr, mfr, er);
 *
 *  c.output(record);
 * }</pre>
 */
@SuppressWarnings("ConstantConditions")
@Builder
public class OccurrenceHdfsRecordTransform implements Serializable {

  private static final long serialVersionUID = 4605359346756029672L;

  // Core
  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;

  @NonNull private final TupleTag<GbifIdRecord> gbifIdRecordTag;
  @NonNull private final TupleTag<ClusteringRecord> clusteringRecordTag;
  @NonNull private final TupleTag<BasicRecord> basicRecordTag;
  @NonNull private final TupleTag<TemporalRecord> temporalRecordTag;
  @NonNull private final TupleTag<LocationRecord> locationRecordTag;
  @NonNull private final TupleTag<TaxonRecord> taxonRecordTag;
  @NonNull private final TupleTag<GrscicollRecord> grscicollRecordTag;
  @NonNull private final TupleTag<EventCoreRecord> eventCoreRecordTag;

  // Extension
  @NonNull private final TupleTag<MultimediaRecord> multimediaRecordTag;
  @NonNull private final TupleTag<ImageRecord> imageRecordTag;
  @NonNull private final TupleTag<AudubonRecord> audubonRecordTag;

  @NonNull private final PCollectionView<MetadataRecord> metadataView;

  public SingleOutput<KV<String, CoGbkResult>, OccurrenceHdfsRecord> converter() {

    DoFn<KV<String, CoGbkResult>, OccurrenceHdfsRecord> fn =
        new DoFn<KV<String, CoGbkResult>, OccurrenceHdfsRecord>() {

          private final Counter counter =
              Metrics.counter(OccurrenceHdfsRecordTransform.class, AVRO_TO_HDFS_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // Core
            MetadataRecord mdr = c.sideInput(metadataView);
            GbifIdRecord id = v.getOnly(gbifIdRecordTag);
            ClusteringRecord cr =
                v.getOnly(clusteringRecordTag, ClusteringRecord.newBuilder().setId(k).build());
            ExtendedRecord er =
                v.getOnly(extendedRecordTag, ExtendedRecord.newBuilder().setId(k).build());
            BasicRecord br = v.getOnly(basicRecordTag, BasicRecord.newBuilder().setId(k).build());
            TemporalRecord tr =
                v.getOnly(temporalRecordTag, TemporalRecord.newBuilder().setId(k).build());
            LocationRecord lr =
                v.getOnly(locationRecordTag, LocationRecord.newBuilder().setId(k).build());
            TaxonRecord txr = v.getOnly(taxonRecordTag, TaxonRecord.newBuilder().setId(k).build());
            GrscicollRecord gr =
                v.getOnly(grscicollRecordTag, GrscicollRecord.newBuilder().setId(k).build());
            // Extension
            MultimediaRecord mr =
                v.getOnly(multimediaRecordTag, MultimediaRecord.newBuilder().setId(k).build());
            ImageRecord ir = v.getOnly(imageRecordTag, ImageRecord.newBuilder().setId(k).build());
            AudubonRecord ar =
                v.getOnly(audubonRecordTag, AudubonRecord.newBuilder().setId(k).build());

            EventCoreRecord eventCoreRecord =
                v.getOnly(eventCoreRecordTag, EventCoreRecord.newBuilder().setId(k).build());

            MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);
            OccurrenceHdfsRecord record =
                OccurrenceHdfsRecordConverter.builder()
                    .basicRecord(br)
                    .gbifIdRecord(id)
                    .clusteringRecord(cr)
                    .metadataRecord(mdr)
                    .temporalRecord(tr)
                    .locationRecord(lr)
                    .taxonRecord(txr)
                    .grscicollRecord(gr)
                    .multimediaRecord(mmr)
                    .extendedRecord(er)
                    .eventCoreRecord(eventCoreRecord)
                    .build()
                    .convert();

            c.output(record);

            counter.inc();
          }
        };

    return ParDo.of(fn).withSideInputs(metadataView);
  }

  /**
   * Writes {@link OccurrenceHdfsRecord} *.avro files to path, data will be split into several
   * files, uses Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public AvroIO.Write<OccurrenceHdfsRecord> write(String toPath, Integer numShards) {
    AvroIO.Write<OccurrenceHdfsRecord> write =
        AvroIO.write(OccurrenceHdfsRecord.class)
            .to(toPath)
            .withSuffix(PipelinesVariables.Pipeline.AVRO_EXTENSION)
            .withCodec(Transform.getBaseCodec());
    return numShards == null ? write : write.withNumShards(numShards);
  }
}
