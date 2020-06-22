package org.gbif.pipelines.fragmenter.record;

import java.util.Optional;

import org.gbif.converters.parser.xml.model.IdentifierRecord;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;

public class XmlOccurrenceRecord implements OccurrenceRecord {

  private final RawXmlOccurrence xmlOccurrence;
  private final RawOccurrenceRecord rawOccurrence;

  private XmlOccurrenceRecord(RawXmlOccurrence xmlOccurrence) {
    this.xmlOccurrence = xmlOccurrence;
    this.rawOccurrence = Optional.ofNullable(xmlOccurrence)
        .flatMap(x -> XmlFragmentParser.parseRecord(xmlOccurrence).stream().findFirst())
        .orElse(null);
  }

  public static XmlOccurrenceRecord create(RawXmlOccurrence xmlOccurrence) {
    return new XmlOccurrenceRecord(xmlOccurrence);
  }

  @Override
  public String toStringRecord() {
    return xmlOccurrence.getXml();
  }

  @Override
  public String getInstitutionCode() {
    return rawOccurrence.getInstitutionCode();
  }

  @Override
  public String getCollectionCode() {
    return rawOccurrence.getCollectionCode();
  }

  @Override
  public String getCatalogNumber() {
    return rawOccurrence.getCatalogueNumber();
  }

  @Override
  public String getOccurrenceId() {
    if (rawOccurrence.getIdentifierRecords() != null) {
      return rawOccurrence.getIdentifierRecords().stream()
          .filter(ir -> ir.getIdentifierType() == IdentifierRecord.OCCURRENCE_ID_TYPE)
          .map(IdentifierRecord::getIdentifier)
          .findFirst()
          .orElse(null);
    }
    return null;
  }
}
