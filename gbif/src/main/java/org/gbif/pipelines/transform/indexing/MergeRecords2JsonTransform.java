package org.gbif.pipelines.transform.indexing;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.indexing.converter.GbifRecords2JsonConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeRecords2JsonTransform extends PTransform<PCollectionTuple, PCollection<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(MergeRecords2JsonTransform.class);

  private final TupleTag<KV<String, InterpretedExtendedRecord>> interKvTag =
      new TupleTag<KV<String, InterpretedExtendedRecord>>() {};
  private final TupleTag<KV<String, TemporalRecord>> temporalKvTag =
      new TupleTag<KV<String, TemporalRecord>>() {};
  private final TupleTag<KV<String, LocationRecord>> locationKvTag =
      new TupleTag<KV<String, LocationRecord>>() {};
  private final TupleTag<KV<String, TaxonRecord>> taxonomyKvTag =
      new TupleTag<KV<String, TaxonRecord>>() {};
  private final TupleTag<KV<String, MultimediaRecord>> multimediaKvTag =
      new TupleTag<KV<String, MultimediaRecord>>() {};

  private final TupleTag<ExtendedRecord> extendedRecordTag = new TupleTag<ExtendedRecord>() {};
  private final TupleTag<InterpretedExtendedRecord> interRecordTag =
      new TupleTag<InterpretedExtendedRecord>() {};
  private final TupleTag<TemporalRecord> temporalTag = new TupleTag<TemporalRecord>() {};
  private final TupleTag<LocationRecord> locationTag = new TupleTag<LocationRecord>() {};
  private final TupleTag<TaxonRecord> taxonomyTag = new TupleTag<TaxonRecord>() {};
  private final TupleTag<MultimediaRecord> multimediaTag = new TupleTag<MultimediaRecord>() {};

  public static MergeRecords2JsonTransform create() {
    return new MergeRecords2JsonTransform();
  }

  @Override
  public PCollection<String> expand(PCollectionTuple input) {

    LOG.info("Adding step 3: Converting to a json object");
    // convert extended records to KV collection
    PCollection<KV<String, ExtendedRecord>> verbatimRecordsMapped =
        input
            .get(extendedRecordTag)
            .apply(
                "Map verbatim records to KV",
                MapElements.into(new TypeDescriptor<KV<String, ExtendedRecord>>() {})
                    .via((ExtendedRecord ex) -> KV.of(ex.getId(), ex)));

    // group all the collections
    PCollection<KV<String, CoGbkResult>> groupedCollection =
        KeyedPCollectionTuple.of(interRecordTag, input.get(interKvTag))
            .and(temporalTag, input.get(temporalKvTag))
            .and(locationTag, input.get(locationKvTag))
            .and(taxonomyTag, input.get(taxonomyKvTag))
            .and(multimediaTag, input.get(multimediaKvTag))
            .and(extendedRecordTag, verbatimRecordsMapped)
            .apply(CoGroupByKey.create());

    return groupedCollection.apply(
        "Merge objects",
        ParDo.of(
            new DoFn<KV<String, CoGbkResult>, String>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                CoGbkResult value = c.element().getValue();
                String key = c.element().getKey();
                InterpretedExtendedRecord interRecord =
                    value.getOnly(
                        interRecordTag, InterpretedExtendedRecord.newBuilder().setId(key).build());
                TemporalRecord temporal =
                    value.getOnly(temporalTag, TemporalRecord.newBuilder().setId(key).build());
                LocationRecord location =
                    value.getOnly(locationTag, LocationRecord.newBuilder().setId(key).build());
                TaxonRecord taxon =
                    value.getOnly(taxonomyTag, TaxonRecord.newBuilder().setId(key).build());
                MultimediaRecord multimedia =
                    value.getOnly(multimediaTag, MultimediaRecord.newBuilder().setId(key).build());
                ExtendedRecord extendedRecord =
                    value.getOnly(
                        extendedRecordTag, ExtendedRecord.newBuilder().setId(key).build());
                c.output(
                    GbifRecords2JsonConverter.create(
                            interRecord, temporal, location, taxon, multimedia, extendedRecord)
                        .buildJson());
              }
            }));
  }

  public MergeRecords2JsonTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(
        pipeline,
        ExtendedRecord.class,
        InterpretedExtendedRecord.class,
        TemporalRecord.class,
        LocationRecord.class,
        TaxonRecord.class,
        MultimediaRecord.class);
    return this;
  }

  public TupleTag<KV<String, InterpretedExtendedRecord>> getInterKvTag() {
    return interKvTag;
  }

  public TupleTag<KV<String, TemporalRecord>> getTemporalKvTag() {
    return temporalKvTag;
  }

  public TupleTag<KV<String, LocationRecord>> getLocationKvTag() {
    return locationKvTag;
  }

  public TupleTag<KV<String, TaxonRecord>> getTaxonomyKvTag() {
    return taxonomyKvTag;
  }

  public TupleTag<KV<String, MultimediaRecord>> getMultimediaKvTag() {
    return multimediaKvTag;
  }

  public TupleTag<ExtendedRecord> getExtendedRecordTag() {
    return extendedRecordTag;
  }
}
