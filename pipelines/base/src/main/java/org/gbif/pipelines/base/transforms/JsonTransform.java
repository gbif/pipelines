package org.gbif.pipelines.base.transforms;

import org.gbif.pipelines.core.converters.GbifJsonConverter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

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

public class JsonTransform extends PTransform<PCollectionTuple, PCollection<String>> {

  private final TupleTag<KV<String, MetadataRecord>> metadataKvTag =
      new TupleTag<KV<String, MetadataRecord>>() {};
  private final TupleTag<KV<String, BasicRecord>> basicKvTag =
      new TupleTag<KV<String, BasicRecord>>() {};
  private final TupleTag<KV<String, TemporalRecord>> temporalKvTag =
      new TupleTag<KV<String, TemporalRecord>>() {};
  private final TupleTag<KV<String, LocationRecord>> locationKvTag =
      new TupleTag<KV<String, LocationRecord>>() {};
  private final TupleTag<KV<String, TaxonRecord>> taxonomyKvTag =
      new TupleTag<KV<String, TaxonRecord>>() {};
  private final TupleTag<KV<String, MultimediaRecord>> multimediaKvTag =
      new TupleTag<KV<String, MultimediaRecord>>() {};

  private final TupleTag<ExtendedRecord> erTag = new TupleTag<ExtendedRecord>() {};
  private final TupleTag<BasicRecord> brTag = new TupleTag<BasicRecord>() {};
  private final TupleTag<TemporalRecord> trTag = new TupleTag<TemporalRecord>() {};
  private final TupleTag<LocationRecord> lrTag = new TupleTag<LocationRecord>() {};
  private final TupleTag<TaxonRecord> txrTag = new TupleTag<TaxonRecord>() {};
  private final TupleTag<MultimediaRecord> mrTag = new TupleTag<MultimediaRecord>() {};

  public static JsonTransform create() {
    return new JsonTransform();
  }

  @Override
  public PCollection<String> expand(PCollectionTuple input) {

    // Metadata singleton collection
    PCollectionView<MetadataRecord> metadataView =
        input.get(metadataKvTag).apply(Values.create()).apply(View.asSingleton());

    // Convert extended records to KV collection
    PCollection<KV<String, ExtendedRecord>> verbatimRecordsMapped =
        input
            .get(erTag)
            .apply(
                "Map verbatim records to KV",
                MapElements.into(new TypeDescriptor<KV<String, ExtendedRecord>>() {})
                    .via((ExtendedRecord ex) -> KV.of(ex.getId(), ex)));

    // Group all collections
    PCollection<KV<String, CoGbkResult>> groupedCollection =
        KeyedPCollectionTuple.of(brTag, input.get(basicKvTag))
            .and(trTag, input.get(temporalKvTag))
            .and(lrTag, input.get(locationKvTag))
            .and(txrTag, input.get(taxonomyKvTag))
            .and(mrTag, input.get(multimediaKvTag))
            .and(erTag, verbatimRecordsMapped)
            .apply(CoGroupByKey.create());

    // Create doFn
    DoFn<KV<String, CoGbkResult>, String> doFn =
        new DoFn<KV<String, CoGbkResult>, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            //
            CoGbkResult value = c.element().getValue();
            String key = c.element().getKey();

            MetadataRecord mdr = c.sideInput(metadataView);
            ExtendedRecord er = value.getOnly(erTag, ExtendedRecord.newBuilder().setId(key).build());
            BasicRecord br = value.getOnly(brTag, BasicRecord.newBuilder().setId(key).build());
            TemporalRecord tr = value.getOnly(trTag, TemporalRecord.newBuilder().setId(key).build());
            LocationRecord lr = value.getOnly(lrTag, LocationRecord.newBuilder().setId(key).build());
            TaxonRecord txr = value.getOnly(txrTag, TaxonRecord.newBuilder().setId(key).build());
            MultimediaRecord mr = value.getOnly(mrTag, MultimediaRecord.newBuilder().setId(key).build());

            //
            String json = GbifJsonConverter.create(mdr, br, tr, lr, txr, mr, er).buildJson().toString();

            c.output(json);
          }
        };

    // Convert to json
    return groupedCollection.apply("Merge objects", ParDo.of(doFn).withSideInputs(metadataView));
  }

  public TupleTag<KV<String, BasicRecord>> getBasicKvTag() {
    return basicKvTag;
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

  public TupleTag<KV<String, MetadataRecord>> getMetadataKvTag() {
    return metadataKvTag;
  }

  public TupleTag<ExtendedRecord> getExtendedRecordTag() {
    return erTag;
  }
}
