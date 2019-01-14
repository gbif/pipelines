package org.gbif.pipelines.transforms;

import java.util.Iterator;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.DUPLICATE_RECORDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.UNIQUE_RECORDS_COUNT;

/** Transformation for filtering all duplicate records with the same {@link ExtendedRecord#getId} */
public class UniqueIdTransform extends PTransform<PCollection<ExtendedRecord>, PCollection<ExtendedRecord>> {

  private static final Logger LOG = LoggerFactory.getLogger(UniqueIdTransform.class);

  private UniqueIdTransform() {}

  public static UniqueIdTransform create() {
    return new UniqueIdTransform();
  }

  @Override
  public PCollection<ExtendedRecord> expand(PCollection<ExtendedRecord> input) {

    // Convert from list to map where, key - occurrenceId, value - object instance and group by key
    PCollection<KV<String, Iterable<ExtendedRecord>>> groupedCollection =
        input
            .apply("Mapping to KV", MapTransforms.extendedToKv())
            .apply("Grouping by occurrenceId", GroupByKey.create());

    // Filter duplicate occurrenceIds, all groups where value size != 1
    return groupedCollection.apply(
        "Filtering duplicates",
        ParDo.of(
            new DoFn<KV<String, Iterable<ExtendedRecord>>, ExtendedRecord>() {

              private final Counter uniqueCounter = Metrics.counter(UniqueIdTransform.class, UNIQUE_RECORDS_COUNT);
              private final Counter duplicateCounter = Metrics.counter(UniqueIdTransform.class, DUPLICATE_RECORDS_COUNT);

              @ProcessElement
              public void processElement(ProcessContext c) {
                KV<String, Iterable<ExtendedRecord>> element = c.element();
                Iterator<ExtendedRecord> iterator = element.getValue().iterator();
                ExtendedRecord record = iterator.next();
                if (!iterator.hasNext()) {
                  c.output(record);
                  uniqueCounter.inc();
                } else {
                  LOG.warn("occurrenceId = {}, duplicates were found", element.getKey());
                  duplicateCounter.inc();
                }
              }
            }));
  }
}
