package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.ABSENT_GBIF_ID_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.FILTERED_GBIF_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_ABSENT;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.gbif.pipelines.io.avro.IdentifierRecord;

@Slf4j
@Getter
@AllArgsConstructor(staticName = "create")
public class GbifIdTupleTransform
    extends PTransform<PCollection<IdentifierRecord>, PCollectionTuple> {

  private final TupleTag<IdentifierRecord> tag = new TupleTag<IdentifierRecord>() {};

  private final TupleTag<IdentifierRecord> absentTag = new TupleTag<IdentifierRecord>() {};

  @Override
  public PCollectionTuple expand(PCollection<IdentifierRecord> input) {

    // Convert from list to map where, key - occurrenceId, value - object instance and group by key
    return input.apply(
        "Filtering duplicates",
        ParDo.of(new Filter()).withOutputTags(tag, TupleTagList.of(absentTag)));
  }

  private class Filter extends DoFn<IdentifierRecord, IdentifierRecord> {

    private final Counter filteredCounter =
        Metrics.counter(GbifIdTupleTransform.class, FILTERED_GBIF_IDS_COUNT);
    private final Counter absentCounter =
        Metrics.counter(GbifIdTupleTransform.class, ABSENT_GBIF_ID_COUNT);

    @ProcessElement
    public void processElement(ProcessContext c) {
      IdentifierRecord ir = c.element();
      if (ir != null) {
        if (ir.getInternalId() == null && ir.getIssues().getIssueList().contains(GBIF_ID_ABSENT)) {
          c.output(absentTag, ir);
          absentCounter.inc();
        } else {
          c.output(tag, ir);
          filteredCounter.inc();
        }
      }
    }
  }
}
