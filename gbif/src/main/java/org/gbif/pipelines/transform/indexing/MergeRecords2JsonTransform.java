package org.gbif.pipelines.transform.indexing;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.indexing.converter.GbifRecords2JsonConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;

public class MergeRecords2JsonTransform extends PTransform<PCollectionTuple, PCollection<String>> {

  private final TupleTag<KV<String, MetadataRecord>> metadataKvTag =
      new TupleTag<KV<String, MetadataRecord>>() {};
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

  private final TupleTag<ExtendedRecord> extendedTag = new TupleTag<ExtendedRecord>() {};
  private final TupleTag<InterpretedExtendedRecord> interpretedTag =
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

    // Metadata singleton collection
    PCollectionView<MetadataRecord> metadataView =
        input.get(metadataKvTag).apply(Values.create()).apply(View.asSingleton());

    // Convert extended records to KV collection
    PCollection<KV<String, ExtendedRecord>> verbatimRecordsMapped =
        input
            .get(extendedTag)
            .apply(
                "Map verbatim records to KV",
                MapElements.into(new TypeDescriptor<KV<String, ExtendedRecord>>() {})
                    .via((ExtendedRecord ex) -> KV.of(ex.getId(), ex)));

    // Group all collections
    PCollection<KV<String, CoGbkResult>> groupedCollection =
        KeyedPCollectionTuple.of(interpretedTag, input.get(interKvTag))
            .and(temporalTag, input.get(temporalKvTag))
            .and(locationTag, input.get(locationKvTag))
            .and(taxonomyTag, input.get(taxonomyKvTag))
            .and(multimediaTag, input.get(multimediaKvTag))
            .and(extendedTag, verbatimRecordsMapped)
            .apply(CoGroupByKey.create());

    // Convert to json
    return groupedCollection.apply(
        "Merge objects",
        ParDo.of(
                new DoFn<KV<String, CoGbkResult>, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    //
                    CoGbkResult value = c.element().getValue();
                    String key = c.element().getKey();
                    //
                    String json =
                        GbifRecords2JsonConverter.create(
                                c.sideInput(metadataView),
                                value.getOnly(interpretedTag, emptyInterpreted(key)),
                                value.getOnly(temporalTag, emptyTemporal(key)),
                                value.getOnly(locationTag, emptyLocation(key)),
                                value.getOnly(taxonomyTag, emptyTaxonomy(key)),
                                value.getOnly(multimediaTag, emptyMultimedia(key)),
                                value.getOnly(extendedTag, emptyExtendedRecord(key)))
                            .buildJson();

                    c.output(json);
                  }
                })
            .withSideInputs(metadataView));
  }

  public MergeRecords2JsonTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(
        pipeline,
        ExtendedRecord.class,
        InterpretedExtendedRecord.class,
        TemporalRecord.class,
        LocationRecord.class,
        TaxonRecord.class,
        MultimediaRecord.class,
        MetadataRecord.class);
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

  public TupleTag<KV<String, MetadataRecord>> getMetadataTag() {
    return metadataKvTag;
  }

  public TupleTag<ExtendedRecord> getExtendedRecordTag() {
    return extendedTag;
  }

  private InterpretedExtendedRecord emptyInterpreted(String key) {
    return InterpretedExtendedRecord.newBuilder().setId(key).build();
  }

  private TemporalRecord emptyTemporal(String key) {
    return TemporalRecord.newBuilder().setId(key).build();
  }

  private LocationRecord emptyLocation(String key) {
    return LocationRecord.newBuilder().setId(key).build();
  }

  private TaxonRecord emptyTaxonomy(String key) {
    return TaxonRecord.newBuilder().setId(key).build();
  }

  private MultimediaRecord emptyMultimedia(String key) {
    return MultimediaRecord.newBuilder().setId(key).build();
  }

  private ExtendedRecord emptyExtendedRecord(String key) {
    return ExtendedRecord.newBuilder().setId(key).build();
  }
}
