package org.gbif.pipelines.fragmenter.record;

import static com.fasterxml.jackson.databind.MapperFeature.SORT_PROPERTIES_ALPHABETICALLY;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.keygen.OccurrenceRecord;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

@Slf4j
@AllArgsConstructor(staticName = "create")
public class DwcaExtensionOccurrenceRecord implements OccurrenceRecord {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    // to ensure that identical records are serialized identically

    MAPPER.configure(SORT_PROPERTIES_ALPHABETICALLY, true);
  }

  @NonNull private final Record core;

  @NonNull private final Record occurrenceExtension;

  @Override
  public Optional<String> getTriplet() {
    String ic = getTerm(DwcTerm.institutionCode);
    String cc = getTerm(DwcTerm.collectionCode);
    String cn = getTerm(DwcTerm.catalogNumber);
    return OccurrenceKeyBuilder.buildKey(ic, cc, cn);
  }

  @Override
  public Optional<String> getOccurrenceId() {
    String term = getTerm(DwcTerm.occurrenceID);
    if (term == null || term.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(term);
  }

  @Override
  public String getStringRecord() {
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
      // while the extension  declares the same terms, but provides no value.
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
