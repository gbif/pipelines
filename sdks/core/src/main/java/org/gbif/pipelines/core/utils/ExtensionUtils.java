package org.gbif.pipelines.core.utils;

import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareOptValue;
import static org.gbif.pipelines.core.utils.ModelUtils.hasExtension;

import java.util.HashSet;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.ObisTerm;
import org.gbif.pipelines.core.pojo.MoFData;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExtensionUtils {

  public static MoFData convertMoFFromVerbatim(ExtendedRecord verbatim) {
    if (verbatim == null || verbatim.getExtensions() == null) {
      return MoFData.builder().build();
    }

    Set<String> measurementTypes = new HashSet<>();
    Set<String> measurementTypeIDs = new HashSet<>();
    if (hasExtension(verbatim, Extension.MEASUREMENT_OR_FACT.getRowType())) {
      verbatim
          .getExtensions()
          .get(Extension.MEASUREMENT_OR_FACT.getRowType())
          .forEach(
              e -> {
                extractNullAwareOptValue(e, DwcTerm.measurementType)
                    .ifPresent(measurementTypes::add);
              });
    }

    if (hasExtension(verbatim, Extension.EXTENDED_MEASUREMENT_OR_FACT.getRowType())) {
      verbatim
          .getExtensions()
          .get(Extension.EXTENDED_MEASUREMENT_OR_FACT.getRowType())
          .forEach(
              e -> {
                extractNullAwareOptValue(e, DwcTerm.measurementType)
                    .ifPresent(measurementTypes::add);
                extractNullAwareOptValue(e, ObisTerm.measurementTypeID)
                    .ifPresent(measurementTypeIDs::add);
              });
    }

    return MoFData.builder()
        .measurementTypes(measurementTypes)
        .measurementTypeIDs(measurementTypeIDs)
        .build();
  }
}
