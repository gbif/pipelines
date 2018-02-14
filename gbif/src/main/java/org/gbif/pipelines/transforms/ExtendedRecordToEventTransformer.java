package org.gbif.pipelines.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Event;
import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.TemporalInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This function converts an extended record to an interpreted KeyValue of occurrenceId and Event.
 * This function returns multiple outputs,
 * a. Interpreted version of raw temporal data as KV<String,Event>
 * b. Issues and lineages applied on raw data to get the interpreted result, as KV<String,IssueLineageRecord>
 */
public class ExtendedRecordToEventTransformer extends DoFn<ExtendedRecord, KV<String, Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtendedRecordToEventTransformer.class);
    /**
     * tags for locating different types of outputs send by this function
     */
    private final TupleTag<KV<String, Event>> eventDataTag = new TupleTag<KV<String, Event>>() {
    };
    private final TupleTag<KV<String, IssueLineageRecord>> eventIssueTag = new TupleTag<KV<String, IssueLineageRecord>>() {
    };

    @ProcessElement
    public void processElement(ProcessContext ctx) {
        ExtendedRecord record = ctx.element();
        Event event = new Event();

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
        IssueLineageRecord issueLineageRecord = Interpretation
                .of(record)
                .using(TemporalInterpreter.interpretDay(event))
                .using(TemporalInterpreter.interpretMonth(event))
                .using(TemporalInterpreter.interpretYear(event)).getIssueLineageRecord(record.getId());

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
        ctx.output(eventDataTag, KV.of(event.getOccurrenceID().toString(), event));
        ctx.output(eventIssueTag, KV.of(event.getOccurrenceID().toString(), issueLineageRecord));
    }

    public TupleTag<KV<String, Event>> getEventDataTag() {
        return eventDataTag;
    }

    public TupleTag<KV<String, IssueLineageRecord>> getEventIssueTag() {
        return eventIssueTag;
    }
}
