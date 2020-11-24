package org.gbif.pipelines.fragmenter.record;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

@Slf4j
@AllArgsConstructor(staticName = "create")
public class DwcaExtensionOccurrenceRecord implements OccurrenceRecord {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    // to ensure that identical records are serialized identically
    MAPPER.configure(SerializationConfig.Feature.SORT_PROPERTIES_ALPHABETICALLY, true);
  }

  @NonNull private final Record core;

  @NonNull private final Record occurrenceExtension;

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

    data.put("id", core.id());

    // Put in all core terms
    for (Term term : core.terms()) {
      data.put(term.simpleName(), core.value(term));
    }

    // overlay them with extension occ terms
    for (Term term : occurrenceExtension.terms()) {
      // do not overwrite values with a NULL.  It can be the case that e.g. Taxon core has values,
      // while the extension
      // declares the same terms, but provides no value.
      if (!StringUtils.isBlank(occurrenceExtension.value(term))) {
        data.put(term.simpleName(), occurrenceExtension.value(term));
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
    String value = occurrenceExtension.value(term);
    if (Strings.isNullOrEmpty(value)) {
      value = core.value(term);
    }
    return value;
  }
}
