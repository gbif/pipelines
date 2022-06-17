package org.gbif.pipelines.fragmenter.record;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.gbif.converters.parser.xml.model.IdentifierRecord;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;
import org.gbif.pipelines.keygen.OccurrenceRecord;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

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
  public Optional<String> getOccurrenceId() {
    if (rawOccurrence.getIdentifierRecords() == null) {
      return Optional.empty();
    }
    return rawOccurrence.getIdentifierRecords().stream()
        .filter(ir -> ir.getIdentifierType() == IdentifierRecord.OCCURRENCE_ID_TYPE)
        .map(IdentifierRecord::getIdentifier)
        .findFirst();
  }

  @Override
  public Optional<String> getTriplet() {
    String ic = rawOccurrence.getInstitutionCode();
    String cc = rawOccurrence.getCollectionCode();
    String cn = rawOccurrence.getCatalogueNumber();
    return OccurrenceKeyBuilder.buildKey(ic, cc, cn);
  }
}
