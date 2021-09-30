package au.org.ala.pipelines.converters;

import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.gbif.pipelines.io.avro.MultimediaIndexRecord;

public class MultimediaCsvConverter {

  private static final TsvConverter<MultimediaIndexRecord> CONVERTER =
      TsvConverter.<MultimediaIndexRecord>create()
          .addKeyTermFn(DwcTerm.occurrenceID, ir -> Optional.of(ir.getId()))
          .addKeyTermFn(DcTerm.identifier, ir -> Optional.of(ir.getIdentifier()))
          .addKeyTermFn(DcTerm.creator, ir -> Optional.ofNullable(ir.getCreator()))
          .addKeyTermFn(DcTerm.created, ir -> Optional.ofNullable(ir.getCreated()))
          .addKeyTermFn(DcTerm.title, ir -> Optional.ofNullable(ir.getTitle()))
          .addKeyTermFn(DcTerm.format, ir -> Optional.ofNullable(ir.getFormat()))
          .addKeyTermFn(DcTerm.license, ir -> Optional.ofNullable(ir.getLicense()))
          .addKeyTermFn(DcTerm.rights, ir -> Optional.ofNullable(ir.getRights()))
          .addKeyTermFn(DcTerm.rightsHolder, ir -> Optional.ofNullable(ir.getRightsHolder()))
          .addKeyTermFn(DcTerm.references, ir -> Optional.ofNullable(ir.getReferences()));

  public static List<String> convert(IndexRecord indexRecord, String imageServiceUrlFormat) {
    return updateIdentifiers(indexRecord, imageServiceUrlFormat).getMultimedia().stream()
        .map(ir -> CONVERTER.converter(ir))
        .collect(Collectors.toList());
  }

  public static IndexRecord updateIdentifiers(IndexRecord ir, String imageServiceUrlFormat) {
    ir.getMultimedia()
        .forEach(
            mir ->
                mir.setIdentifier(
                    MessageFormat.format(imageServiceUrlFormat, mir.getIdentifier())));
    return ir;
  }

  public static List<String> getTerms() {
    return CONVERTER.getTerms();
  }
}
