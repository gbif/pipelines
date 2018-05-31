package org.gbif.pipelines.mapper;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import java.util.function.Function;

public class TemporalRecordMapper {

  private TemporalRecordMapper() {
    //Can't have an instance
  }

  /**
   * Base fields mapping
   */
  public static TemporalRecord map(ExtendedRecord record) {
    Function<DwcTerm, String> getValue = dwcterm -> record.getCoreTerms().get(dwcterm.qualifiedName());
    return TemporalRecord.newBuilder()
      .setId(record.getId())
      .setEventID(getValue.apply(DwcTerm.eventID))
      .setParentEventID(getValue.apply(DwcTerm.parentEventID))
      .setFieldNumber(getValue.apply(DwcTerm.fieldNumber))
      .setVerbatimEventDate(getValue.apply(DwcTerm.verbatimEventDate))
      .setEventTime(getValue.apply(DwcTerm.eventTime))
      .setFieldNotes(getValue.apply(DwcTerm.fieldNotes))
      .setEventRemarks(getValue.apply(DwcTerm.eventRemarks))
      .setHabitat(getValue.apply(DwcTerm.habitat))
      .setSamplingProtocol(getValue.apply(DwcTerm.samplingProtocol))
      .setSampleSizeValue(getValue.apply(DwcTerm.sampleSizeValue))
      .setSampleSizeUnit(getValue.apply(DwcTerm.sampleSizeUnit))
      .setSamplingEffort(getValue.apply(DwcTerm.samplingEffort))
      .build();
  }

}
