package org.gbif.pipelines.ingest.pipelines.fragmenter;

import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.fragmenter.record.DwcaOccurrenceRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;

@Slf4j
@NoArgsConstructor(staticName = "create")
public class DwcaOccurrenceRecordConverterFn extends DoFn<ExtendedRecord, DwcaOccurrenceRecord> {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  @ProcessElement
  public void processElement(@Element ExtendedRecord er, OutputReceiver<DwcaOccurrenceRecord> out) {

    String triplet = getTriplet(er);
    String occurrenceId = getOccurrenceId(er);
    String stringRecord = getStringRecord(er);

    out.output(new DwcaOccurrenceRecord(triplet, occurrenceId, stringRecord));
  }

  private static String getTriplet(ExtendedRecord er) {
    String ic = extractNullAwareValue(er, DwcTerm.institutionCode);
    String cc = extractNullAwareValue(er, DwcTerm.collectionCode);
    String cn = extractNullAwareValue(er, DwcTerm.catalogNumber);
    return OccurrenceKeyBuilder.buildKey(ic, cc, cn).orElse(null);
  }

  private static String getOccurrenceId(ExtendedRecord er) {
    return extractNullAwareValue(er, DwcTerm.occurrenceID);
  }

  private static String getStringRecord(ExtendedRecord er) {
    // we need alphabetically sorted maps to guarantee that identical records have identical JSON
    Map<String, Object> data = new TreeMap<>();

    data.put("id", er.getId());

    // Put in all core terms
    er.getCoreTerms()
        .forEach(
            (t, value) -> {
              String ct = TERM_FACTORY.findTerm(t).simpleName();
              data.put(ct, value);
            });

    if (!er.getExtensions().isEmpty()) {
      Map<Term, List<Map<String, String>>> extensions =
          new TreeMap<>(Comparator.comparing(Term::qualifiedName));
      data.put("extensions", extensions);

      // iterate over extensions
      er.getExtensions()
          .forEach(
              (ex, values) -> {
                List<Map<String, String>> records =
                    new ArrayList<>(er.getExtensions().get(ex).size());

                // iterate over extensions records
                values.forEach(
                    m -> {
                      Map<String, String> edata = new TreeMap<>();

                      m.forEach(
                          (t, value) -> {
                            String evt = TERM_FACTORY.findTerm(t).simpleName();
                            edata.put(evt, value);
                          });
                      records.add(edata);
                    });

                Term et = TERM_FACTORY.findTerm(ex);
                extensions.put(et, records);
              });
    }
    // serialize to json
    try {
      return MAPPER.writeValueAsString(data);
    } catch (IOException e) {
      log.error("Cannot serialize star record data", e);
    }
    return "";
  }
}
