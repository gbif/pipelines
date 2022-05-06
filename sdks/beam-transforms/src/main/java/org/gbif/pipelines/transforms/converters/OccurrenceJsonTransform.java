package org.gbif.pipelines.transforms.converters;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;

import java.io.Serializable;
import java.util.function.UnaryOperator;
import lombok.Builder;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
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
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.core.converters.ParentJsonConverter;
import org.gbif.pipelines.core.utils.HashConverter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.io.avro.json.ParentJsonRecord;

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
public class OccurrenceJsonTransform implements Serializable {

  private static final long serialVersionUID = 1279313931024806171L;

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  private static final String BASE_NAME = "occurrence_json";

  // Core
  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;
  @NonNull private final TupleTag<GbifIdRecord> gbifIdRecordTag;
  @NonNull private final TupleTag<ClusteringRecord> clusteringRecordTag;
  @NonNull private final TupleTag<BasicRecord> basicRecordTag;
  @NonNull private final TupleTag<TemporalRecord> temporalRecordTag;
  @NonNull private final TupleTag<LocationRecord> locationRecordTag;
  @NonNull private final TupleTag<TaxonRecord> taxonRecordTag;
  @NonNull private final TupleTag<GrscicollRecord> grscicollRecordTag;
  // Extension
  @NonNull private final TupleTag<MultimediaRecord> multimediaRecordTag;
  @NonNull private final TupleTag<ImageRecord> imageRecordTag;
  @NonNull private final TupleTag<AudubonRecord> audubonRecordTag;

  @NonNull private final PCollectionView<MetadataRecord> metadataView;

  public SingleOutput<KV<String, CoGbkResult>, OccurrenceJsonRecord> converter() {

    DoFn<KV<String, CoGbkResult>, OccurrenceJsonRecord> fn =
        new DoFn<KV<String, CoGbkResult>, OccurrenceJsonRecord>() {

          private final Counter counter =
              Metrics.counter(OccurrenceJsonTransform.class, AVRO_TO_JSON_COUNT);

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

            MultimediaRecord mmr = MultimediaConverter.merge(mr, ir, ar);
            OccurrenceJsonRecord json =
                OccurrenceJsonConverter.builder()
                    .metadata(mdr)
                    .gbifId(id)
                    .clustering(cr)
                    .basic(br)
                    .temporal(tr)
                    .location(lr)
                    .taxon(txr)
                    .grscicoll(gr)
                    .multimedia(mmr)
                    .verbatim(er)
                    .build()
                    .convert();

            c.output(json);

            counter.inc();
          }
        };

    return ParDo.of(fn).withSideInputs(metadataView);
  }

  public static String getBaseName() {
    return BASE_NAME;
  }

  public static Schema getAvroSchema() {
    return OccurrenceJsonRecord.SCHEMA$;
  }

  public static SingleOutput<OccurrenceJsonRecord, ParentJsonRecord> parentJsonRecordConverter() {
    DoFn<OccurrenceJsonRecord, ParentJsonRecord> fn =
        new DoFn<OccurrenceJsonRecord, ParentJsonRecord>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(ParentJsonRecord.newBuilder().setOccurrence(c.element()).build());
          }
        };

    return ParDo.of(fn);
  }

  public static SingleOutput<OccurrenceJsonRecord, String> jsonConverter() {

    DoFn<OccurrenceJsonRecord, String> fn =
        new DoFn<OccurrenceJsonRecord, String>() {

          private final Counter counter =
              Metrics.counter(OccurrenceJsonTransform.class, AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(c.element().toString());
            counter.inc();
          }
        };

    return ParDo.of(fn);
  }

  public static SingleOutput<OccurrenceJsonRecord, String> jsonParentRecordConverter() {

    DoFn<OccurrenceJsonRecord, String> fn =
        new DoFn<OccurrenceJsonRecord, String>() {

          private final Counter counter =
              Metrics.counter(OccurrenceJsonTransform.class, AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(
                ParentJsonConverter.builder()
                    .occurrenceJsonRecord(c.element())
                    .identifier(
                        IdentifierRecord.newBuilder()
                            .setInternalId(
                                HashConverter.getSha1(
                                    c.element().getDatasetKey(),
                                    c.element().getVerbatim().getParentCoreId()))
                            .setUniqueKey(c.element().getOccurrenceId())
                            .setId(c.element().getId())
                            .build())
                    .build()
                    .toJson());
            counter.inc();
          }
        };

    return ParDo.of(fn);
  }

  public static class Read {

    /**
     * Reads avro files from path, which contains {@link OccurrenceJsonRecord}
     *
     * @param path path to source files
     */
    public static AvroIO.Read<OccurrenceJsonRecord> read(String path) {
      return AvroIO.read(OccurrenceJsonRecord.class).from(path);
    }

    /**
     * Reads avro files from path, which contains {@link OccurrenceJsonRecord}
     *
     * @param pathFn function can return an output path, where in param is fixed - {@link
     *     OccurrenceJsonTransform#BASE_NAME}
     */
    public static AvroIO.Read<OccurrenceJsonRecord> read(UnaryOperator<String> pathFn) {
      return read(pathFn.apply(BASE_NAME));
    }
  }

  public static class Write {

    /**
     * Writes {@link OccurrenceJsonRecord} *.avro files to path, data will be split into several
     * files, uses Snappy compression codec by default
     *
     * @param toPath path with name to output files, like - directory/name
     */
    public static AvroIO.Write<OccurrenceJsonRecord> write(String toPath) {
      return AvroIO.write(OccurrenceJsonRecord.class)
          .to(toPath)
          .withSuffix(PipelinesVariables.Pipeline.AVRO_EXTENSION)
          .withCodec(BASE_CODEC);
    }

    /**
     * Writes {@link OccurrenceJsonRecord} *.avro files to path, data will be split into several
     * files, uses Snappy compression codec by default
     *
     * @param pathFn function can return an output path, where in param is fixed - {@link
     *     OccurrenceJsonTransform#BASE_NAME}
     */
    public static AvroIO.Write<OccurrenceJsonRecord> write(UnaryOperator<String> pathFn) {
      return write(pathFn.apply(BASE_NAME));
    }
  }
}
