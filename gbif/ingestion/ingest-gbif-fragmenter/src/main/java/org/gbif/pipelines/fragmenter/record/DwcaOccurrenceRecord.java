package org.gbif.pipelines.fragmenter.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.keygen.OccurrenceRecord;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

@Slf4j
public class DwcaOccurrenceRecord implements OccurrenceRecord {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    // to ensure that identical records are serialized identically
    MAPPER.configure(SerializationConfig.Feature.SORT_PROPERTIES_ALPHABETICALLY, true);
  }

  public final String triplet;
  public final String occurrenceId;
  public final String stringRecord;

  public DwcaOccurrenceRecord(String triplet, String occurrenceId, String stringRecord) {
    this.triplet = triplet;
    this.occurrenceId = occurrenceId;
    this.stringRecord = stringRecord;
  }

  private DwcaOccurrenceRecord(StarRecord sr) {
    this.triplet = readTriplet(sr);
    this.occurrenceId = readOccurrenceId(sr);
    this.stringRecord = readStringRecord(sr);
  }

  public static DwcaOccurrenceRecord create(StarRecord sr) {
    return new DwcaOccurrenceRecord(sr);
  }

  @Override
  public Optional<String> getTriplet() {
    return Optional.ofNullable(triplet);
  }

  @Override
  public Optional<String> getOccurrenceId() {
    return Optional.ofNullable(occurrenceId);
  }

  @Override
  public String getStringRecord() {
    return stringRecord;
  }

  private String readTriplet(StarRecord sr) {
    String ic = getTerm(sr, DwcTerm.institutionCode);
    String cc = getTerm(sr, DwcTerm.collectionCode);
    String cn = getTerm(sr, DwcTerm.catalogNumber);
    return OccurrenceKeyBuilder.buildKey(ic, cc, cn).orElse(null);
  }

  private String readOccurrenceId(StarRecord sr) {
    String term = getTerm(sr, DwcTerm.occurrenceID);
    return term == null || term.isEmpty() ? null : term;
  }

  private String readStringRecord(StarRecord sr) {
    // we need alphabetically sorted maps to guarantee that identical records have identical JSON
    Map<String, Object> data = new TreeMap<>();

    data.put("id", sr.core().id());

    // Put in all core terms
    for (Term term : sr.core().terms()) {
      String value = sr.core().value(term);
      if (value != null && !value.isEmpty()) {
        data.put(term.simpleName(), value);
      }
    }

    if (!sr.extensions().isEmpty()) {
      Map<Term, List<Map<String, String>>> extensions =
          new TreeMap<>(Comparator.comparing(Term::qualifiedName));
      data.put("extensions", extensions);

      // iterate over extensions
      for (Term rowType : sr.extensions().keySet()) {
        List<Map<String, String>> records = new ArrayList<>(sr.extension(rowType).size());
        extensions.put(rowType, records);

        // iterate over extension records
        for (Record erec : sr.extension(rowType)) {
          Map<String, String> edata = new TreeMap<>();
          records.add(edata);
          for (Term term : erec.terms()) {
            String value = erec.value(term);
            if (value != null && !value.isEmpty()) {
              edata.put(term.simpleName(), value);
            }
          }
        }
      }
    }
    // serialize to json
    try {
      return MAPPER.writeValueAsString(data);
    } catch (IOException e) {
      log.error("Cannot serialize star record data", e);
    }
    return "";
  }

  private String getTerm(StarRecord sr, DwcTerm term) {
    return sr.core().value(term);
  }
}
