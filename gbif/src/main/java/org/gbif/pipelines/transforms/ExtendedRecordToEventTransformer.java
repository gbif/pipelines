package org.gbif.pipelines.transforms;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Event;
import org.gbif.pipelines.core.functions.interpretation.InterpretationFactory;
import org.gbif.pipelines.core.functions.interpretation.InterpretationResult;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.TemporalInterpreter;
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
  private final TupleTag<KV<String, Event>> eventDataTag = new TupleTag<KV<String, Event>>(){};
  private final TupleTag<KV<String, IssueLineageRecord>> eventIssueTag = new TupleTag<KV<String, IssueLineageRecord>>(){};
  private static final Logger LOG = LoggerFactory.getLogger(ExtendedRecordToEventTransformer.class);

  @ProcessElement
  public void processElement(ProcessContext ctx) {
    ExtendedRecord record = ctx.element();
    Event event = new Event();

    Map<CharSequence, List<Issue>> fieldIssueMap = new HashMap<>();
    Map<CharSequence, List<Lineage>> fieldLineageMap = new HashMap<>();
    //mapping raw record with interpreted ones
    event.setOccurrenceID(record.getId());

    event.setBasisOfRecord(record.getCoreTerms().get(DwcTerm.basisOfRecord.qualifiedName()));
    event.setEventID(record.getCoreTerms().get(DwcTerm.eventID.qualifiedName()));
    event.setParentEventID(record.getCoreTerms().get(DwcTerm.parentEventID.qualifiedName()));
    event.setFieldNumber(record.getCoreTerms().get(DwcTerm.fieldNumber.qualifiedName()));
    event.setEventDate(record.getCoreTerms().get(DwcTerm.eventDate.qualifiedName()));
    event.setStartDayOfYear(record.getCoreTerms().get(DwcTerm.startDayOfYear.qualifiedName()));
    event.setEndDayOfYear(record.getCoreTerms().get(DwcTerm.endDayOfYear.qualifiedName()));

    /*
      Day month year interpretation
     */

    String rawYear = record.getCoreTerms().get(DwcTerm.year.qualifiedName()).toString();
    String rawMonth = record.getCoreTerms().get(DwcTerm.month.qualifiedName()).toString();
    String rawDay = record.getCoreTerms().get(DwcTerm.day.qualifiedName()).toString();

    Interpretation
      .of(record)
      .using(TemporalInterpreter.interpretDay(event))
      .using(TemporalInterpreter.interpretMonth(event))
      .using(TemporalInterpreter.interpretYear(event));

    final InterpretationResult<Integer> interpretedDay = InterpretationFactory.interpret(DwcTerm.day, rawDay);
    interpretedDay.ifSuccessFulThenElse((e1) -> event.setDay(e1.getResult().orElse(null)),
                                        (e2) -> {
                                          event.setDay(e2.getResult().orElse(null));
                                          fieldIssueMap.put(DwcTerm.day.name(), e2.getIssueList());
                                          fieldLineageMap.put(DwcTerm.day.name(), e2.getLineageList());
                                        });

    final InterpretationResult<Integer> interpretedMonth = InterpretationFactory.interpret(DwcTerm.month, rawMonth);
    interpretedMonth.ifSuccessFulThenElse((e1) -> event.setMonth(e1.getResult().orElse(null)), (e2) -> {
      event.setMonth(e2.getResult().orElse(null));
      fieldIssueMap.put(DwcTerm.month.name(), e2.getIssueList());
      fieldLineageMap.put(DwcTerm.month.name(), e2.getLineageList());
    });

    final InterpretationResult<Integer> interpretedYear = InterpretationFactory.interpret(DwcTerm.year, rawYear);
    interpretedYear.ifSuccessFulThenElse((e1) -> event.setYear(e1.getResult().orElse(null)),
                                         (e2) -> {
                                           event.setYear(e2.getResult().orElse( null));
                                           fieldIssueMap.put(DwcTerm.year.name(), e2.getIssueList());
                                           fieldLineageMap.put(DwcTerm.year.name(), e2.getLineageList());
                                         });

    event.setVerbatimEventDate(record.getCoreTerms().get(DwcTerm.verbatimEventDate.qualifiedName()));
    event.setHabitat(record.getCoreTerms().get(DwcTerm.habitat.qualifiedName()));
    event.setSamplingProtocol(record.getCoreTerms().get(DwcTerm.samplingProtocol.qualifiedName()));
    event.setSamplingEffort(record.getCoreTerms().get(DwcTerm.samplingEffort.qualifiedName()));
    event.setSampleSizeValue(record.getCoreTerms().get(DwcTerm.sampleSizeValue.qualifiedName()));
    event.setSampleSizeUnit(record.getCoreTerms().get(DwcTerm.sampleSizeUnit.qualifiedName()));
    event.setFieldNotes(record.getCoreTerms().get(DwcTerm.fieldNotes.qualifiedName()));
    event.setEventRemarks(record.getCoreTerms().get(DwcTerm.eventRemarks.qualifiedName()));
    event.setInstitutionID(record.getCoreTerms().get(DwcTerm.institutionID.qualifiedName()));
    event.setCollectionID(record.getCoreTerms().get(DwcTerm.collectionID.qualifiedName()));
    event.setDatasetID(record.getCoreTerms().get(DwcTerm.datasetID.qualifiedName()));
    event.setInstitutionCode(record.getCoreTerms().get(DwcTerm.institutionCode.qualifiedName()));
    event.setCollectionCode(record.getCoreTerms().get(DwcTerm.collectionCode.qualifiedName()));
    event.setDatasetName(record.getCoreTerms().get(DwcTerm.datasetName.qualifiedName()));
    event.setOwnerInstitutionCode(record.getCoreTerms().get(DwcTerm.ownerInstitutionCode.qualifiedName()));
    event.setDynamicProperties(record.getCoreTerms().get(DwcTerm.dynamicProperties.qualifiedName()));
    event.setInformationWithheld(record.getCoreTerms().get(DwcTerm.informationWithheld.qualifiedName()));
    event.setDataGeneralizations(record.getCoreTerms().get(DwcTerm.dataGeneralizations.qualifiedName()));
    //all issues and lineages are dumped on this object
    final IssueLineageRecord finalRecord = IssueLineageRecord.newBuilder()
      .setOccurenceId(record.getId())
      .setFieldIssuesMap(fieldIssueMap)
      .setFieldLineageMap(fieldLineageMap)
      .build();

    ctx.output(eventDataTag, KV.of(event.getOccurrenceID().toString(), event));
    ctx.output(eventIssueTag, KV.of(event.getOccurrenceID().toString(), finalRecord));
  }

  public TupleTag<KV<String, Event>> getEventDataTag() {
    return eventDataTag;
  }

  public TupleTag<KV<String, IssueLineageRecord>> getEventIssueTag() {
    return eventIssueTag;
  }
}
