package org.gbif.pipelines.core.functions.transforms;

import org.gbif.dwca.avro.Event;
import org.gbif.pipelines.core.functions.interpretation.DayInterpreter;
import org.gbif.pipelines.core.functions.interpretation.InterpretationException;
import org.gbif.pipelines.core.functions.interpretation.MonthInterpreter;
import org.gbif.pipelines.core.functions.interpretation.YearInterpreter;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class ExtendedRecordToEventTransformer extends DoFn<ExtendedRecord, KV<String, Event>> {

  /**
   * tags for locating different types of outputs send by this function
   */
  public static final TupleTag<KV<String, Event>> EVENT_DATA_TAG = new TupleTag<>();
  public static final TupleTag<KV<String, IssueLineageRecord>> EVENT_ISSUE_TAG = new TupleTag<>();
  private static final Logger LOG = LoggerFactory.getLogger(ExtendedRecordToEventTransformer.class);

  @ProcessElement
  public void processElement(ProcessContext ctx) {
    ExtendedRecord record = ctx.element();
    Event evt = new Event();

    Map<CharSequence, List<Issue>> fieldIssueMap = new HashMap<>();
    Map<CharSequence, List<Lineage>> fieldLineageMap = new HashMap<>();
    //mapping raw record with interpreted ones
    evt.setOccurrenceID(record.getId());

    evt.setBasisOfRecord(record.getCoreTerms().get(DwCATermIdentifier.basisOfRecord.getIdentifier()));
    evt.setEventID(record.getCoreTerms().get(DwCATermIdentifier.eventID.getIdentifier()));
    evt.setParentEventID(record.getCoreTerms().get(DwCATermIdentifier.parentEventID.getIdentifier()));
    evt.setFieldNumber(record.getCoreTerms().get(DwCATermIdentifier.fieldNumber.getIdentifier()));
    evt.setEventDate(record.getCoreTerms().get(DwCATermIdentifier.eventDate.getIdentifier()));
    evt.setStartDayOfYear(record.getCoreTerms().get(DwCATermIdentifier.startDayOfYear.getIdentifier()));
    evt.setEndDayOfYear(record.getCoreTerms().get(DwCATermIdentifier.endDayOfYear.getIdentifier()));

    /*
      Day month year interpretation
     */
    CharSequence raw_year = record.getCoreTerms().get(DwCATermIdentifier.year.getIdentifier());
    CharSequence raw_month = record.getCoreTerms().get(DwCATermIdentifier.month.getIdentifier());
    CharSequence raw_day = record.getCoreTerms().get(DwCATermIdentifier.day.getIdentifier());

    if (raw_day != null) {
      try {
        evt.setDay(new DayInterpreter().interpret(raw_day.toString()));
      } catch (InterpretationException e) {
        fieldIssueMap.put(DwCATermIdentifier.day.name(), e.getIssues());
        fieldLineageMap.put(DwCATermIdentifier.day.name(), e.getLineages());
        if (e.getInterpretedValue().isPresent()) evt.setDay((Integer) e.getInterpretedValue().get());
      }
    }

    if (raw_month != null) {
      try {
        evt.setMonth(new MonthInterpreter().interpret(raw_month.toString()));
      } catch (InterpretationException e) {
        fieldIssueMap.put(DwCATermIdentifier.month.name(), e.getIssues());
        fieldLineageMap.put(DwCATermIdentifier.month.name(), e.getLineages());
        if (e.getInterpretedValue().isPresent()) evt.setMonth((Integer) e.getInterpretedValue().get());
      }
    }

    if (raw_year != null) {
      try {
        evt.setYear(new YearInterpreter().interpret(raw_year.toString()));
      } catch (InterpretationException e) {
        fieldIssueMap.put(DwCATermIdentifier.year.name(), e.getIssues());
        fieldLineageMap.put(DwCATermIdentifier.year.name(), e.getLineages());
        if (e.getInterpretedValue().isPresent()) evt.setYear((Integer) e.getInterpretedValue().get());
      }
    }

    evt.setVerbatimEventDate(record.getCoreTerms().get(DwCATermIdentifier.verbatimEventDate.getIdentifier()));
    evt.setHabitat(record.getCoreTerms().get(DwCATermIdentifier.habitat.getIdentifier()));
    evt.setSamplingProtocol(record.getCoreTerms().get(DwCATermIdentifier.samplingProtocol.getIdentifier()));
    evt.setSamplingEffort(record.getCoreTerms().get(DwCATermIdentifier.samplingEffort.getIdentifier()));
    evt.setSampleSizeValue(record.getCoreTerms().get(DwCATermIdentifier.sampleSizeValue.getIdentifier()));
    evt.setSampleSizeUnit(record.getCoreTerms().get(DwCATermIdentifier.sampleSizeUnit.getIdentifier()));
    evt.setFieldNotes(record.getCoreTerms().get(DwCATermIdentifier.fieldNotes.getIdentifier()));
    evt.setEventRemarks(record.getCoreTerms().get(DwCATermIdentifier.eventRemarks.getIdentifier()));
    evt.setInstitutionID(record.getCoreTerms().get(DwCATermIdentifier.institutionID.getIdentifier()));
    evt.setCollectionID(record.getCoreTerms().get(DwCATermIdentifier.collectionID.getIdentifier()));
    evt.setDatasetID(record.getCoreTerms().get(DwCATermIdentifier.datasetID.getIdentifier()));
    evt.setInstitutionCode(record.getCoreTerms().get(DwCATermIdentifier.institutionCode.getIdentifier()));
    evt.setCollectionCode(record.getCoreTerms().get(DwCATermIdentifier.collectionCode.getIdentifier()));
    evt.setDatasetName(record.getCoreTerms().get(DwCATermIdentifier.datasetName.getIdentifier()));
    evt.setOwnerInstitutionCode(record.getCoreTerms().get(DwCATermIdentifier.ownerInstitutionCode.getIdentifier()));
    evt.setDynamicProperties(record.getCoreTerms().get(DwCATermIdentifier.dynamicProperties.getIdentifier()));
    evt.setInformationWithheld(record.getCoreTerms().get(DwCATermIdentifier.informationWithheld.getIdentifier()));
    evt.setDataGeneralizations(record.getCoreTerms().get(DwCATermIdentifier.dataGeneralizations.getIdentifier()));
    //all issues and lineages are dumped on this object
    final IssueLineageRecord finalRecord = IssueLineageRecord.newBuilder()
      .setOccurenceId(record.getId())
      .setFieldIssuesMap(fieldIssueMap)
      .setFieldLineageMap(fieldLineageMap)
      .build();

    ctx.output(EVENT_DATA_TAG, KV.of(evt.getOccurrenceID().toString(), evt));
    ctx.output(EVENT_ISSUE_TAG, KV.of(evt.getOccurrenceID().toString(), finalRecord));
  }

}
