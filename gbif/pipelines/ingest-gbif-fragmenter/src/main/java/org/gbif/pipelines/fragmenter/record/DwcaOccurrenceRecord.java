package org.gbif.pipelines.fragmenter.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

@Slf4j
@AllArgsConstructor(staticName = "create")
public class DwcaOccurrenceRecord implements OccurrenceRecord {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    // to ensure that identical records are serialized identically
    MAPPER.configure(SerializationConfig.Feature.SORT_PROPERTIES_ALPHABETICALLY, true);
  }

  @NonNull private final StarRecord starRecord;

  @Override
  public String getInstitutionCode() {
    return getTerm(DwcTerm.institutionCode);
  }

  @Override
  public String getCollectionCode() {
    return getTerm(DwcTerm.collectionCode);
  }

  @Override
  public String getCatalogNumber() {
    return getTerm(DwcTerm.catalogNumber);
  }

  @Override
  public String getOccurrenceId() {
    return getTerm(DwcTerm.occurrenceID);
  }

  @Override
  public String toStringRecord() {
    // we need alphabetically sorted maps to guarantee that identical records have identical JSON
    Map<String, Object> data = new TreeMap<>();

    data.put("id", starRecord.core().id());

    // Put in all core terms
    for (Term term : starRecord.core().terms()) {
      data.put(term.simpleName(), starRecord.core().value(term));
    }

    if (!starRecord.extensions().isEmpty()) {
      Map<Term, List<Map<String, String>>> extensions =
          new TreeMap<>(Comparator.comparing(Term::qualifiedName));
      data.put("extensions", extensions);

      // iterate over extensions
      for (Term rowType : starRecord.extensions().keySet()) {
        List<Map<String, String>> records = new ArrayList<>(starRecord.extension(rowType).size());
        extensions.put(rowType, records);

        // iterate over extension records
        for (Record erec : starRecord.extension(rowType)) {
          Map<String, String> edata = new TreeMap<>();
          records.add(edata);
          for (Term term : erec.terms()) {
            edata.put(term.simpleName(), erec.value(term));
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

  private String getTerm(DwcTerm term) {
    return starRecord.core().value(term);
  }
}
