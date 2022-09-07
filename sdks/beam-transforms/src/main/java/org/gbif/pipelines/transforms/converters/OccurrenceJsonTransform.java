package org.gbif.pipelines.transforms.converters;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;

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
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.core.converters.specific.GbifParentJsonConverter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;

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

  private static final long serialVersionUID = 1279313931024806172L;

  // Core
  @NonNull private final TupleTag<ExtendedRecord> extendedRecordTag;
  @NonNull private final TupleTag<IdentifierRecord> identifierRecordTag;
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

  // Determines if the output record is a parent-child record
  @Builder.Default private final boolean asParentChildRecord = false;

  public SingleOutput<KV<String, CoGbkResult>, String> converter() {

    DoFn<KV<String, CoGbkResult>, String> fn =
        new DoFn<KV<String, CoGbkResult>, String>() {

          private final Counter counter =
              Metrics.counter(OccurrenceJsonTransform.class, AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // Core
            MetadataRecord mdr = c.sideInput(metadataView);
            IdentifierRecord id = v.getOnly(identifierRecordTag);
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
            OccurrenceJsonConverter occurrenceJsonConverter =
                OccurrenceJsonConverter.builder()
                    .metadata(mdr)
                    .identifier(id)
                    .clustering(cr)
                    .basic(br)
                    .temporal(tr)
                    .location(lr)
                    .taxon(txr)
                    .grscicoll(gr)
                    .multimedia(mmr)
                    .verbatim(er)
                    .build();
            if (asParentChildRecord) {
              c.output(
                  GbifParentJsonConverter.builder()
                      .occurrenceJsonRecord(occurrenceJsonConverter.convert())
                      .metadata(mdr)
                      .build()
                      .toJson());
            } else {
              // Occurrence index clients (GraphQL) rely on exinsting fields null vaules
              c.output(occurrenceJsonConverter.toJsonWithNulls());
            }

            counter.inc();
          }
        };

    return ParDo.of(fn).withSideInputs(metadataView);
  }
}
