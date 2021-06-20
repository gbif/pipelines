package au.org.ala.pipelines.converters;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.IndexRecord;

public class MultimediaCsvConverter {

  private static final CsvConverter<IndexRecord> CONVERTER =
      CsvConverter.<IndexRecord>create()
          .addKeyTermFn("id", ir -> Optional.of(ir.getId()))
          .addKeyTermFn(DcTerm.identifier, ir -> Optional.of(ir.getId()))
          .addKeyTermFn(DcTerm.creator, getString(DcTerm.creator))
          .addKeyTermFn(DcTerm.created, getString(DcTerm.created))
          .addKeyTermFn(DcTerm.title, getString(DcTerm.title))
          .addKeyTermFn(DcTerm.format, getString(DcTerm.format))
          .addKeyTermFn(DcTerm.license, getString(DcTerm.license))
          .addKeyTermFn(DcTerm.rights, getString(DcTerm.rights))
          .addKeyTermFn(DcTerm.rightsHolder, getString(DcTerm.rightsHolder))
          .addKeyTermFn(DcTerm.references, getString(DcTerm.references));

  public static String convert(IndexRecord indexRecord) {
    return CONVERTER.converter(indexRecord);
  }

  public static List<String> getTerms() {
    return CONVERTER.getTerms();
  }

  private static Function<IndexRecord, Optional<String>> getString(Term term) {
    return getString(term.simpleName());
  }

  private static Function<IndexRecord, Optional<String>> getString(String key) {
    return ir -> Optional.ofNullable(ir.getStrings().get(key));
  }
}
