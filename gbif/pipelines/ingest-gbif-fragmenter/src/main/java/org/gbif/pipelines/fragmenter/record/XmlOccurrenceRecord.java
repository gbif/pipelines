package org.gbif.pipelines.fragmenter.record;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.gbif.converters.parser.xml.model.IdentifierRecord;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;

@Slf4j
public class XmlOccurrenceRecord implements OccurrenceRecord {

  private final RawXmlOccurrence xmlOccurrence;
  private final RawOccurrenceRecord rawOccurrence;

  private XmlOccurrenceRecord(RawXmlOccurrence xmlOccurrence) {
    this.xmlOccurrence = xmlOccurrence;
    this.rawOccurrence =
        Optional.ofNullable(xmlOccurrence)
            .flatMap(
                x -> {
                  try {
                    return XmlFragmentParser.parseRecord(xmlOccurrence).stream().findFirst();
                  } catch (RuntimeException ex) {
                    log.error(ex.getLocalizedMessage(), ex);
                  }
                  return Optional.empty();
                })
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
    if (rawOccurrence.getIdentifierRecords() == null) {
      return null;
    }
    return rawOccurrence.getIdentifierRecords().stream()
        .filter(ir -> ir.getIdentifierType() == IdentifierRecord.OCCURRENCE_ID_TYPE)
        .map(IdentifierRecord::getIdentifier)
        .findFirst()
        .orElse(null);
  }
}
