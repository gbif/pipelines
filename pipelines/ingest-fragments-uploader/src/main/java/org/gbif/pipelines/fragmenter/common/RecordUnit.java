package org.gbif.pipelines.fragmenter.common;

import java.util.Optional;
import java.util.function.Function;

import org.gbif.converters.parser.xml.model.IdentifierRecord;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.DwcTerm;

import com.google.common.base.Strings;

public class RecordUnit {

  private final StarRecord starRecord;
  private final Record core;
  private final Record occExtension;
  private final RawXmlOccurrence xmlOccurrence;
  private final RawOccurrenceRecord rawOccurrence;

  private RecordUnit(StarRecord starRecord, Record core, Record occExtension, RawXmlOccurrence xmlOccurrence) {
    this.starRecord = starRecord;
    this.core = core;
    this.occExtension = occExtension;
    this.xmlOccurrence = xmlOccurrence;
    this.rawOccurrence = Optional.ofNullable(xmlOccurrence)
        .flatMap(x -> XmlFragmentParser.parseRecord(xmlOccurrence).stream().findFirst())
        .orElse(null);
  }

  public static RecordUnit create(StarRecord starRecord) {
    return new RecordUnit(starRecord, null, null, null);
  }

  public static RecordUnit create(Record core, Record occExtension) {
    return new RecordUnit(null, core, occExtension, null);
  }

  public static RecordUnit create(RawXmlOccurrence xmlOccurrence) {
    return new RecordUnit(null, null, null, xmlOccurrence);
  }

  @Override
  public String toString() {
    if (starRecord != null) {
      return StarRecordSerializer.toJson(starRecord);
    }
    if (core != null && occExtension != null) {
      return StarRecordSerializer.toJson(core, occExtension);
    }
    if (xmlOccurrence != null) {
      return xmlOccurrence.getXml();
    }
    return "";
  }

  public String getInstitutionCode() {
    return getTerm(DwcTerm.institutionCode, RawOccurrenceRecord::getInstitutionCode);
  }

  public String getCollectionCode() {
    return getTerm(DwcTerm.collectionCode, RawOccurrenceRecord::getCollectionCode);
  }

  public String getCatalogNumber() {
    return getTerm(DwcTerm.catalogNumber, RawOccurrenceRecord::getCatalogueNumber);
  }

  public String getOccurrenceId() {

    Function<RawOccurrenceRecord, String> xmlFn = roc -> {
      if (roc.getIdentifierRecords() != null) {
        return roc.getIdentifierRecords().stream()
            .filter(ir -> ir.getIdentifierType() == IdentifierRecord.OCCURRENCE_ID_TYPE)
            .map(IdentifierRecord::getIdentifier)
            .findFirst()
            .orElse(null);
      }
      return null;
    };

    return getTerm(DwcTerm.occurrenceID, xmlFn);
  }

  private String getTerm(DwcTerm term, Function<RawOccurrenceRecord, String> xmlFn) {
    if (starRecord != null) {
      return starRecord.core().value(term);
    }
    if (core != null && occExtension != null) {
      String value = occExtension.value(term);
      if (Strings.isNullOrEmpty(value)) {
        value = core.value(term);
      }
      return value;
    }
    if (rawOccurrence != null) {
      return xmlFn.apply(rawOccurrence);
    }
    return null;
  }

}
