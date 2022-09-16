package org.gbif.pipelines.core.utils;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.DwcFiles;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.pojo.FileNameTerm;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DwcaUtils {

  private static final Map<String, String> EXTENSION_TYPE_MAP = new HashMap<>();

  static {
    EXTENSION_TYPE_MAP.put(
        Extension.MEASUREMENT_OR_FACT.getRowType(), RecordType.MEASUREMENT_OR_FACT_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.IDENTIFICATION.getRowType(), RecordType.IDENTIFICATION_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.RESOURCE_RELATIONSHIP.getRowType(),
        RecordType.RESOURCE_RELATIONSHIP_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.AMPLIFICATION.getRowType(), RecordType.AMPLIFICATION_TABLE.name());
    EXTENSION_TYPE_MAP.put(Extension.CLONING.getRowType(), RecordType.CLONING_TABLE.name());
    EXTENSION_TYPE_MAP.put(Extension.GEL_IMAGE.getRowType(), RecordType.GEL_IMAGE_TABLE.name());
    EXTENSION_TYPE_MAP.put(Extension.LOAN.getRowType(), RecordType.LOAN_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.MATERIAL_SAMPLE.getRowType(), RecordType.MATERIAL_SAMPLE_TABLE.name());
    EXTENSION_TYPE_MAP.put(Extension.PERMIT.getRowType(), RecordType.PERMIT_TABLE.name());
    EXTENSION_TYPE_MAP.put(Extension.PREPARATION.getRowType(), RecordType.PREPARATION_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.PRESERVATION.getRowType(), RecordType.PRESERVATION_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.GERMPLASM_MEASUREMENT_SCORE.getRowType(),
        RecordType.GERMPLASM_MEASUREMENT_SCORE_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.GERMPLASM_MEASUREMENT_TRAIT.getRowType(),
        RecordType.GERMPLASM_MEASUREMENT_TRAIT_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.GERMPLASM_MEASUREMENT_TRIAL.getRowType(),
        RecordType.GERMPLASM_MEASUREMENT_TRIAL_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.GERMPLASM_ACCESSION.getRowType(), RecordType.GERMPLASM_ACCESSION_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.EXTENDED_MEASUREMENT_OR_FACT.getRowType(),
        RecordType.EXTENDED_MEASUREMENT_OR_FACT_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.CHRONOMETRIC_AGE.getRowType(), RecordType.CHRONOMETRIC_AGE_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.CHRONOMETRIC_DATE.getRowType(), RecordType.CHRONOMETRIC_DATE_TABLE.name());
    EXTENSION_TYPE_MAP.put(Extension.REFERENCE.getRowType(), RecordType.REFERENCE_TABLE.name());
    EXTENSION_TYPE_MAP.put(Extension.IDENTIFIER.getRowType(), RecordType.IDENTIFIER_TABLE.name());
    EXTENSION_TYPE_MAP.put(Extension.AUDUBON.getRowType(), RecordType.AUDUBON_TABLE.name());
    EXTENSION_TYPE_MAP.put(Extension.IMAGE.getRowType(), RecordType.IMAGE_TABLE.name());
    EXTENSION_TYPE_MAP.put(Extension.MULTIMEDIA.getRowType(), RecordType.MULTIMEDIA_TABLE.name());
    EXTENSION_TYPE_MAP.put(
        Extension.DNA_DERIVED_DATA.getRowType(), RecordType.DNA_DERIVED_DATA_TABLE.name());
  }

  @SneakyThrows
  public static Archive fromLocation(Path path) {
    return DwcFiles.fromLocation(path);
  }

  @SneakyThrows
  public static Archive fromLocationSkipValidation(Path path) {
    return DwcFiles.fromLocationSkipValidation(path);
  }

  @SneakyThrows
  public static Archive fromCompressed(Path path, Path workingDir) {
    return DwcFiles.fromCompressed(path, workingDir);
  }

  public static Set<String> getExtensionAsTerms(Archive archive) {
    return archive.getExtensions().stream()
        .map(x -> EXTENSION_TYPE_MAP.get(x.getRowType().qualifiedName()))
        .collect(Collectors.toSet());
  }

  public static Map<FileNameTerm, Set<Term>> getCoreTerms(Archive archive) {
    archive.validate();
    return Collections.singletonMap(
        FileNameTerm.create(
            archive.getCore().getTitle(), archive.getCore().getRowType().qualifiedName()),
        archive.getCore().getTerms());
  }

  public static Map<FileNameTerm, Set<Term>> getExtensionsTerms(Archive archive) {
    return archive.getExtensions().stream()
        .collect(
            Collectors.toMap(
                af -> FileNameTerm.create(af.getTitle(), af.getRowType().qualifiedName()),
                ArchiveFile::getTerms,
                (a, b) -> b));
  }
}
