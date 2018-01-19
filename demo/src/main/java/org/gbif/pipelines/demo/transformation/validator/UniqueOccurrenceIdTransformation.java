package org.gbif.pipelines.demo.transformation.validator;

import org.gbif.pipelines.demo.transformation.ValidatorsTransformation;
import org.gbif.pipelines.io.avro.UntypedOccurrenceLowerCase;

import java.util.stream.StreamSupport;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transformation for filtering all duplicate records with the same occurrenceId
 *
 * TODO: Probably it should not be UntypedOccurrenceLowerCase class, some common class with occurrenceID as a key.
 */
public class UniqueOccurrenceIdTransformation extends ValidatorsTransformation<UntypedOccurrenceLowerCase> {

  private static final Logger LOG = LoggerFactory.getLogger(UniqueOccurrenceIdTransformation.class);

  private static final String MAP_STEP = "Mapping to KV";
  private static final String GROUP_STEP = "Group by occurrenceId";
  private static final String FILTER_STEP = "Filter duplicates";

  private final TupleTag<UntypedOccurrenceLowerCase> dataTag = new TupleTag<UntypedOccurrenceLowerCase>() {};
  private final TupleTag<KV<String, Iterable<UntypedOccurrenceLowerCase>>> issueTag =
    new TupleTag<KV<String, Iterable<UntypedOccurrenceLowerCase>>>() {};

  @Override
  public PCollectionTuple expand(PCollection<UntypedOccurrenceLowerCase> input) {

    //Convert from list to map where, key - occurrenceId, value - object instance
    PCollection<KV<String, UntypedOccurrenceLowerCase>> map = input.apply(MAP_STEP
      , MapElements.into(new TypeDescriptor<KV<String, UntypedOccurrenceLowerCase>>() {})
        .via((UntypedOccurrenceLowerCase uo) -> KV.of(uo.getOccurrenceid().toString(), uo)));

    //Group map by key - occurrenceId
    PCollection<KV<String, Iterable<UntypedOccurrenceLowerCase>>> group = map.apply(GROUP_STEP, GroupByKey.create());

    //Filter duplicate occurrenceIds, all groups where value size != 1
    return group.apply(FILTER_STEP
      , ParDo.of(new DoFn<KV<String, Iterable<UntypedOccurrenceLowerCase>>, UntypedOccurrenceLowerCase>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          KV<String, Iterable<UntypedOccurrenceLowerCase>> element = c.element();
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

  public TupleTag<UntypedOccurrenceLowerCase> getDataTag() {
    return dataTag;
  }

  public TupleTag<KV<String, Iterable<UntypedOccurrenceLowerCase>>> getIssueTag() {
    return issueTag;
  }
}

