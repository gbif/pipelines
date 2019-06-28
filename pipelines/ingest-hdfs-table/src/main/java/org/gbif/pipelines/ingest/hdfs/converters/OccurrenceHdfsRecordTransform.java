package org.gbif.pipelines.ingest.hdfs.converters;

import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.io.AvroIO;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.converters.MultimediaConverter;
import org.gbif.pipelines.io.avro.*;

import java.io.Serializable;
import java.util.function.UnaryOperator;

import lombok.AllArgsConstructor;
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
import org.gbif.pipelines.transforms.core.BasicTransform;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_HDFS_COUNT;

/**
 * Apache Beam functions and I/O methods to handle {@link OccurrenceHdfsRecord}.
 */
public class OccurrenceHdfsRecordTransform  {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  private static final String BASE_NAME = "hdfsview";

  /**
   * Reads avro files from path, which contains {@link OccurrenceHdfsRecord}
   *
   * @param path path to source files
   */
  public static AvroIO.Read<OccurrenceHdfsRecord> read(String path) {
    return AvroIO.read(OccurrenceHdfsRecord.class).from(path);
  }


  /* Writes {@link OccurrenceHdfsRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param toPath path with name to output files, like - directory/name
   */
  public static AvroIO.Write<OccurrenceHdfsRecord> write(String toPath) {
    return AvroIO.write(OccurrenceHdfsRecord.class).to(toPath).withSuffix(PipelinesVariables.Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC);
  }

  /**
   * Writes {@link BasicRecord} *.avro files to path, data will be split into several files, uses
   * Snappy compression codec by default
   *
   * @param pathFn function can return an output path, where in param is fixed - {@link BasicTransform#BASE_NAME}
   */
  public static AvroIO.Write<OccurrenceHdfsRecord> write(UnaryOperator<String> pathFn) {
    return write(pathFn.apply(BASE_NAME));
  }

  /**
   * Beam level transformation for Occurrence HDFS Downloads Table. The transformation consumes objects, which classes were generated
   * from avro schema files and converts into json string object
   *
   * <p>
   * Example:
   * <p>
   *
   * <pre>{@code
   *
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
   * OccurrenceHdfsRecord record = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(mdr, br, tr, lr, txr, mmr, mfr, er);
   *
   *  c.output(record);
   * }</pre>
   */
  @AllArgsConstructor(staticName = "create")
  public static class Transform implements Serializable {

    private static final long serialVersionUID = 1279313931024806169L;

    // Core
    @NonNull
    private final TupleTag<ExtendedRecord> erTag;
    @NonNull
    private final TupleTag<BasicRecord> brTag;
    @NonNull
    private final TupleTag<TemporalRecord> trTag;
    @NonNull
    private final TupleTag<LocationRecord> lrTag;
    @NonNull
    private final TupleTag<TaxonRecord> txrTag;
    // Extension
    @NonNull
    private final TupleTag<MultimediaRecord> mrTag;
    @NonNull
    private final TupleTag<ImageRecord> irTag;
    @NonNull
    private final TupleTag<AudubonRecord> arTag;
    @NonNull
    private final TupleTag<MeasurementOrFactRecord> mfrTag;

    @NonNull
    private final PCollectionView<MetadataRecord> metadataView;

    public SingleOutput<KV<String, CoGbkResult>, OccurrenceHdfsRecord> converter() {

      DoFn<KV<String, CoGbkResult>, OccurrenceHdfsRecord> fn = new DoFn<KV<String, CoGbkResult>, OccurrenceHdfsRecord>() {

        private final Counter counter = Metrics.counter(OccurrenceHdfsRecordTransform.class, AVRO_TO_HDFS_COUNT);

        @ProcessElement
        public void processElement(ProcessContext c) {
          CoGbkResult v = c.element().getValue();
          String k = c.element().getKey();

          // Core
          MetadataRecord mdr = c.sideInput(metadataView);
          ExtendedRecord er = v.getOnly(erTag, ExtendedRecord.newBuilder().setId(k).build());
          BasicRecord br = v.getOnly(brTag, BasicRecord.newBuilder().setId(k).build());
          TemporalRecord tr = v.getOnly(trTag, TemporalRecord.newBuilder().setId(k).build());
          LocationRecord lr = v.getOnly(lrTag, LocationRecord.newBuilder().setId(k).build());
          TaxonRecord txr = v.getOnly(txrTag, TaxonRecord.newBuilder().setId(k).build());
          // Extension
          MultimediaRecord mr = v.getOnly(mrTag, MultimediaRecord.newBuilder().setId(k).build());
          ImageRecord ir = v.getOnly(irTag, ImageRecord.newBuilder().setId(k).build());
          AudubonRecord ar = v.getOnly(arTag, AudubonRecord.newBuilder().setId(k).build());
          MeasurementOrFactRecord mfr = v.getOnly(mfrTag, MeasurementOrFactRecord.newBuilder().setId(k).build());

          MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);
          OccurrenceHdfsRecord record = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(mdr, br, tr, lr, txr, mmr, mfr, er);

          c.output(record);

          counter.inc();
        }
      };

      return ParDo.of(fn).withSideInputs(metadataView);
    }
  }
}
