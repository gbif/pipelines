package org.gbif.pipelines.demo.transform.validator;

import org.gbif.pipelines.core.utils.Mapper;
import org.gbif.pipelines.demo.transform.ValidatorsTransform;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.stream.StreamSupport;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transformation for filtering all duplicate records with the same record id
 */
public class UniqueOccurrenceIdTransform extends ValidatorsTransform<ExtendedRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(UniqueOccurrenceIdTransform.class);

  private static final String MAP_STEP = "Mapping to KV";
  private static final String GROUP_STEP = "Group by occurrenceId";
  private static final String FILTER_STEP = "Filter duplicates";

  private final TupleTag<ExtendedRecord> dataTag = new TupleTag<ExtendedRecord>() {};
  private final TupleTag<KV<String, Iterable<ExtendedRecord>>> issueTag =
    new TupleTag<KV<String, Iterable<ExtendedRecord>>>() {};

  @Override
  public PCollectionTuple expand(PCollection<ExtendedRecord> input) {

    //Convert from list to map where, key - occurrenceId, value - object instance
    PCollection<KV<String, ExtendedRecord>> map =
      input.apply(MAP_STEP, Mapper.via((ExtendedRecord uo) -> KV.of(uo.getId(), uo)));

    //Group map by key - occurrenceId
    PCollection<KV<String, Iterable<ExtendedRecord>>> group = map.apply(GROUP_STEP, GroupByKey.create());

    //Filter duplicate occurrenceIds, all groups where value size != 1
    return group.apply(FILTER_STEP, ParDo.of(new DoFn<KV<String, Iterable<ExtendedRecord>>, ExtendedRecord>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        KV<String, Iterable<ExtendedRecord>> element = c.element();
        long count = StreamSupport.stream(element.getValue().spliterator(), false).count();
        if (count == 1) {
          element.getValue().forEach(x -> c.output(dataTag, x));
        } else {
          c.output(issueTag, element);
          LOG.warn("occurrenceId = {}, duplicate found = {}", element.getKey(), count);
        }
      }
    }).withOutputTags(dataTag, TupleTagList.of(issueTag)));
  }

  public TupleTag<ExtendedRecord> getDataTag() {
    return dataTag;
  }

  public TupleTag<KV<String, Iterable<ExtendedRecord>>> getIssueTag() {
    return issueTag;
  }
}

