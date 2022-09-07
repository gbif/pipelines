package org.gbif.pipelines.transforms.common;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.DUPLICATE_GBIF_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.IDENTICAL_GBIF_OBJECTS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.INVALID_GBIF_ID_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.UNIQUE_GBIF_IDS_COUNT;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.core.utils.HashConverter;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;

/**
 * Splits collection into two: 1 - normal collection with regular GBIF ids 2 - contains invalid
 * records withGBIF ids, as duplicates or missed GBIF ids
 */
@Slf4j
@Getter
@AllArgsConstructor(staticName = "create")
public class UniqueGbifIdTransform
    extends PTransform<PCollection<IdentifierRecord>, PCollectionTuple> {

  private final TupleTag<IdentifierRecord> tag = new TupleTag<IdentifierRecord>() {};
  private final TupleTag<IdentifierRecord> invalidTag = new TupleTag<IdentifierRecord>() {};

  // Skip transform dynamically
  private final boolean skipTransform;

  public static UniqueGbifIdTransform create() {
    return new UniqueGbifIdTransform(false);
  }

  @Override
  public PCollectionTuple expand(PCollection<IdentifierRecord> input) {

    if (skipTransform) {
      return PCollectionTuple.of(tag, input)
          .and(
              invalidTag,
              Create.empty(TypeDescriptor.of(IdentifierRecord.class))
                  .expand(PBegin.in(input.getPipeline())));
    }

    // Convert from list to map where, key - occurrenceId, value - object instance and group by key
    PCollection<KV<String, Iterable<IdentifierRecord>>> groupedCollection =
        input
            .apply("Mapping to KV", GbifIdTransform.builder().create().toGbifIdKv())
            .apply("Grouping by gbifId", GroupByKey.create());

    // Filter duplicate occurrenceIds, all groups where value size != 1
    return groupedCollection.apply(
        "Filtering duplicates",
        ParDo.of(new Filter()).withOutputTags(tag, TupleTagList.of(invalidTag)));
  }

  private class Filter extends DoFn<KV<String, Iterable<IdentifierRecord>>, IdentifierRecord> {

    private final Counter uniqueCounter =
        Metrics.counter(UniqueGbifIdTransform.class, UNIQUE_GBIF_IDS_COUNT);
    private final Counter duplicateCounter =
        Metrics.counter(UniqueGbifIdTransform.class, DUPLICATE_GBIF_IDS_COUNT);
    private final Counter identicalCounter =
        Metrics.counter(UniqueGbifIdTransform.class, IDENTICAL_GBIF_OBJECTS_COUNT);
    private final Counter invalidCounter =
        Metrics.counter(UniqueGbifIdTransform.class, INVALID_GBIF_ID_COUNT);

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<String, Iterable<IdentifierRecord>> element = c.element();
      Iterator<IdentifierRecord> iterator = element.getValue().iterator();
      IdentifierRecord next = iterator.next();

      if (!iterator.hasNext()) {
        // No duplicates were found, but can be invalid GBIF id

        if (next.getInternalId() == null) {
          log.warn("GBIF ID DOESN'T EXIST - {}", next);
          invalidCounter.inc();
          c.output(invalidTag, next);
        } else {
          uniqueCounter.inc();
          c.output(tag, next);
        }

      } else {
        // Found duplicates, compare all duplicate records, maybe they are identical
        Map<String, IdentifierRecord> map = new TreeMap<>();
        map.put(HashConverter.getSha1(next.getId()), next);

        while (iterator.hasNext()) {
          IdentifierRecord ir = iterator.next();
          map.put(HashConverter.getSha1(ir.getId()), ir);
        }

        List<IdentifierRecord> records = new LinkedList<>(map.values());

        if (records.size() == 1 && records.get(0) == null) {
          log.warn("GBIF ID DOESN'T EXIST - {}", next);
          invalidCounter.inc();
          c.output(invalidTag, next);
        } else if (records.size() > 1) {

          int skip = -1;
          for (int x = 0; x < records.size(); x++) {
            if (records.get(x).getInternalId() != null) {
              skip = x;
              break;
            }
          }

          if (skip > -1) {
            c.output(tag, records.remove(skip));
          }
          records.forEach(x -> c.output(invalidTag, x));

        } else {
          c.output(tag, next);
          uniqueCounter.inc();
          identicalCounter.inc();
        }

        // Log duplicate and metric
        log.warn("gbifId = {}, duplicates were found", element.getKey());
        duplicateCounter.inc(map.size());
      }
    }
  }
}
