package org.gbif.pipelines.base.transforms;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Iterator;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transformation for filtering all duplicate records with the same record id */
public class UniqueIdTransform
    extends PTransform<PCollection<ExtendedRecord>, PCollection<ExtendedRecord>> {

  private static final Logger LOG = LoggerFactory.getLogger(UniqueIdTransform.class);

  private UniqueIdTransform() {}

  public static UniqueIdTransform create() {
    return new UniqueIdTransform();
  }

  @Override
  public PCollection<ExtendedRecord> expand(PCollection<ExtendedRecord> input) {

    // Convert from list to map where, key - occurrenceId, value - object instance
    PCollection<KV<String, ExtendedRecord>> map =
        input.apply(
            "Mapping to KV",
            MapElements.into(new TypeDescriptor<KV<String, ExtendedRecord>>() {})
                .via((ExtendedRecord uo) -> KV.of(uo.getId(), uo)));

    // Group map by key - occurrenceId
    PCollection<KV<String, Iterable<ExtendedRecord>>> group =
        map.apply("Group by occurrenceId", GroupByKey.create());

    // Filter duplicate occurrenceIds, all groups where value size != 1
    return group.apply(
        "Filter duplicates",
        ParDo.of(
            new DoFn<KV<String, Iterable<ExtendedRecord>>, ExtendedRecord>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                KV<String, Iterable<ExtendedRecord>> element = c.element();
                Iterator<ExtendedRecord> iterator = element.getValue().iterator();
                ExtendedRecord record = iterator.next();
                if (!iterator.hasNext()) {
                  c.output(record);
                } else {
                  LOG.warn("occurrenceId = {}, duplicates were found", element.getKey());
                }
              }
            }));
  }
}
