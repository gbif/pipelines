package org.gbif.pipelines.mapper;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.Event;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.function.Function;

public class EventMapper {

  private EventMapper() {
    //Can't have an instance
  }

  public static Event map(ExtendedRecord record) {
    Function<DwcTerm, String> getValue = dwcterm -> record.getCoreTerms().get(dwcterm.qualifiedName());
    return Event.newBuilder()
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

}
