package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.common.parsers.NumberParser;
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
    evt.setBasisOfRecord(record.getCoreTerms().get(DwCATermIdentifier.basisOfRecord.getIdentifier()));
    evt.setEventID(record.getCoreTerms().get(DwCATermIdentifier.eventID.getIdentifier()));
    evt.setParentEventID(record.getCoreTerms().get(DwCATermIdentifier.parentEventID.getIdentifier()));
    evt.setFieldNumber(record.getCoreTerms().get(DwCATermIdentifier.fieldNumber.getIdentifier()));
    evt.setEventDate(record.getCoreTerms().get(DwCATermIdentifier.eventDate.getIdentifier()));
    evt.setStartDayOfYear(record.getCoreTerms().get(DwCATermIdentifier.startDayOfYear.getIdentifier()));
    evt.setEndDayOfYear(record.getCoreTerms().get(DwCATermIdentifier.endDayOfYear.getIdentifier()));

    /**
     * Day month year interpretation
     */
    CharSequence raw_year= record.getCoreTerms().get(DwCATermIdentifier.year.getIdentifier());
    CharSequence raw_month= record.getCoreTerms().get(DwCATermIdentifier.month.getIdentifier());
    CharSequence raw_day= record.getCoreTerms().get(DwCATermIdentifier.day.getIdentifier());

    Integer interpretedDay = null,interpretedMonth=null,interpretedYear=null;
    if(raw_day!=null){
      interpretedDay = NumberParser.parseInteger(raw_day.toString());
      if(interpretedDay>31 || interpretedDay<1)
        interpretedDay = null;
    }

    if(raw_month!=null){
      interpretedMonth = NumberParser.parseInteger(raw_month.toString());
      if(interpretedMonth>12 || interpretedMonth<1)
        interpretedMonth = null;
    }

    if(raw_year!=null){
      interpretedYear = NumberParser.parseInteger(raw_year.toString());
    }

    evt.setYear(interpretedYear);
    evt.setMonth(interpretedMonth);
    evt.setDay(interpretedDay);


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
    ctx.output(KV.of(evt.getOccurrenceID().toString(),evt));
  }

}
