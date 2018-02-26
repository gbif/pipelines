package org.gbif.pipelines.transform;

import org.gbif.dwca.avro.Event;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.TemporalInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.mapper.EventMapper;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventTransform extends RecordTransform<ExtendedRecord, Event> {

  private static final Logger LOG = LoggerFactory.getLogger(EventTransform.class);

  public EventTransform() {
    super("Interpret event record");
  }

  /**
   *
   * @return
   */
  @Override
  DoFn<ExtendedRecord, KV<String, Event>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, Event>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {
        ExtendedRecord record = context.element();

        Event event = EventMapper.map(record);

        //Day month year interpretation
        IssueLineageRecord issueLineageRecord = Interpretation.of(record)
          .using(TemporalInterpreter.interpretDay(event))
          .using(TemporalInterpreter.interpretMonth(event))
          .using(TemporalInterpreter.interpretYear(event))
          .getIssueLineageRecord(record.getId());

        LOG.debug("Raw records converted to temporal category reporting issues and lineages");

        // Additional output
        context.output(getIssueTag(), KV.of(event.getOccurrenceID(), issueLineageRecord));

        // Main output
        context.output(getDataTag(), KV.of(event.getOccurrenceID(), event));
      }
    };
  }

}
