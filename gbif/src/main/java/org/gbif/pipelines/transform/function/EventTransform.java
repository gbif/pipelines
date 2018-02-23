package org.gbif.pipelines.transform.function;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Event;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.TemporalInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.function.Function;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This function converts an extended record to an interpreted KeyValue of occurrenceId and Event.
 * This function returns multiple outputs,
 * a. Interpreted version of raw temporal data as KV<String,Event>
 * b. Issues and lineages applied on raw data to get the interpreted result, as KV<String,IssueLineageRecord>
 */
public class EventTransform extends DoFn<ExtendedRecord, KV<String, Event>> {

  private static final Logger LOG = LoggerFactory.getLogger(EventTransform.class);
  /**
   * tags for locating different types of outputs send by this function
   */
  private final TupleTag<KV<String, Event>> eventDataTag = new TupleTag<KV<String, Event>>() {};
  private final TupleTag<KV<String, IssueLineageRecord>> eventIssueTag =
    new TupleTag<KV<String, IssueLineageRecord>>() {};

  @ProcessElement
  public void processElement(ProcessContext ctx) {

    ExtendedRecord record = ctx.element();

    Event event = toEvent(record);

    /*
      Day month year interpretation
     */
    IssueLineageRecord issueLineageRecord = Interpretation.of(record)
      .using(TemporalInterpreter.interpretDay(event))
      .using(TemporalInterpreter.interpretMonth(event))
      .using(TemporalInterpreter.interpretYear(event))
      .getIssueLineageRecord(record.getId());
    LOG.debug("Raw records converted to temporal category reporting issues and lineages");
    //all issues and lineages are dumped on this object
    ctx.output(eventDataTag, KV.of(event.getOccurrenceID(), event));
    ctx.output(eventIssueTag, KV.of(event.getOccurrenceID(), issueLineageRecord));
  }

  private static Event toEvent(ExtendedRecord record) {
    Function<DwcTerm, String> getValue = dwcterm -> record.getCoreTerms().get(dwcterm.qualifiedName());
    return Event.newBuilder()
      //mapping raw record with interpreted ones
      .setOccurrenceID(record.getId())
      .setBasisOfRecord(getValue.apply(DwcTerm.basisOfRecord))
      .setEventID(getValue.apply(DwcTerm.eventID))
      .setParentEventID(getValue.apply(DwcTerm.parentEventID))
      .setFieldNumber(getValue.apply(DwcTerm.fieldNumber))
      .setEventDate(getValue.apply(DwcTerm.eventDate))
      .setStartDayOfYear(getValue.apply(DwcTerm.startDayOfYear))
      .setEndDayOfYear(getValue.apply(DwcTerm.endDayOfYear))
      .setVerbatimEventDate(getValue.apply(DwcTerm.verbatimEventDate))
      .setHabitat(getValue.apply(DwcTerm.habitat))
      .setSamplingProtocol(getValue.apply(DwcTerm.samplingProtocol))
      .setSamplingEffort(getValue.apply(DwcTerm.samplingEffort))
      .setSampleSizeValue(getValue.apply(DwcTerm.sampleSizeValue))
      .setSampleSizeUnit(getValue.apply(DwcTerm.sampleSizeUnit))
      .setFieldNotes(getValue.apply(DwcTerm.fieldNotes))
      .setEventRemarks(getValue.apply(DwcTerm.eventRemarks))
      .setInstitutionID(getValue.apply(DwcTerm.institutionID))
      .setCollectionID(getValue.apply(DwcTerm.collectionID))
      .setDatasetID(getValue.apply(DwcTerm.datasetID))
      .setInstitutionCode(getValue.apply(DwcTerm.institutionCode))
      .setCollectionCode(getValue.apply(DwcTerm.collectionCode))
      .setDatasetName(getValue.apply(DwcTerm.datasetName))
      .setOwnerInstitutionCode(getValue.apply(DwcTerm.ownerInstitutionCode))
      .setDynamicProperties(getValue.apply(DwcTerm.dynamicProperties))
      .setInformationWithheld(getValue.apply(DwcTerm.informationWithheld))
      .setDataGeneralizations(getValue.apply(DwcTerm.dataGeneralizations))
      .build();
  }

  public TupleTag<KV<String, Event>> getEventDataTag() {
    return eventDataTag;
  }

  public TupleTag<KV<String, IssueLineageRecord>> getEventIssueTag() {
    return eventIssueTag;
  }
}
