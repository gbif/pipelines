package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Event;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtendedRecordToEventTransformer extends DoFn<ExtendedRecord, KV<String,Event>> {

  private static final Logger LOG = LoggerFactory.getLogger(ExtendedRecordToEventTransformer.class);

  @ProcessElement
  public void processElement(ProcessContext ctx){
    ExtendedRecord record = ctx.element();
    Event evt = new Event();
    evt.setOccurrenceID(record.getId());
    evt.setBasisOfRecord(record.getCoreTerms().get(DwcTerm.basisOfRecord));
    evt.setEventID(record.getCoreTerms().get(DwcTerm.eventID));
    evt.setParentEventID(record.getCoreTerms().get(DwcTerm.parentEventID));
    evt.setFieldNumber(record.getCoreTerms().get(DwcTerm.fieldNumber));
    evt.setEventDate(record.getCoreTerms().get(DwcTerm.eventDate));
    evt.setStartDayOfYear(record.getCoreTerms().get(DwcTerm.startDayOfYear));
    evt.setEndDayOfYear(record.getCoreTerms().get(DwcTerm.endDayOfYear));
    evt.setYear(record.getCoreTerms().get(DwcTerm.year));
    evt.setMonth(record.getCoreTerms().get(DwcTerm.month));
    evt.setDay(record.getCoreTerms().get(DwcTerm.day));
    evt.setVerbatimEventDate(record.getCoreTerms().get(DwcTerm.verbatimEventDate));
    evt.setHabitat(record.getCoreTerms().get(DwcTerm.habitat));
    evt.setSamplingProtocol(record.getCoreTerms().get(DwcTerm.samplingProtocol));
    evt.setSamplingEffort(record.getCoreTerms().get(DwcTerm.samplingEffort));
    evt.setSampleSizeValue(record.getCoreTerms().get(DwcTerm.sampleSizeValue));
    evt.setSampleSizeUnit(record.getCoreTerms().get(DwcTerm.sampleSizeUnit));
    evt.setFieldNotes(record.getCoreTerms().get(DwcTerm.fieldNotes));
    evt.setEventRemarks(record.getCoreTerms().get(DwcTerm.eventRemarks));
    evt.setInstitutionID(record.getCoreTerms().get(DwcTerm.institutionID));
    evt.setCollectionID(record.getCoreTerms().get(DwcTerm.collectionID));
    evt.setDatasetID(record.getCoreTerms().get(DwcTerm.datasetID));
    evt.setInstitutionCode(record.getCoreTerms().get(DwcTerm.institutionCode));
    evt.setCollectionCode(record.getCoreTerms().get(DwcTerm.collectionCode));
    evt.setDatasetName(record.getCoreTerms().get(DwcTerm.datasetName));
    evt.setOwnerInstitutionCode(record.getCoreTerms().get(DwcTerm.ownerInstitutionCode));
    evt.setDynamicProperties(record.getCoreTerms().get(DwcTerm.dynamicProperties));
    evt.setInformationWithheld(record.getCoreTerms().get(DwcTerm.informationWithheld));
    evt.setDataGeneralizations(record.getCoreTerms().get(DwcTerm.dataGeneralizations));
    LOG.info(record.toString());
    //LOG.info(evt.toString());
    ctx.output(KV.of(evt.getOccurrenceID().toString(),evt));
  }

}
