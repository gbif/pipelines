package org.gbif.pipelines.core.converters;

import java.util.Optional;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactTable;

/**
 * Converter for the MeasurementsOrFacts extension, converts/map form {@link ExtendedRecord} to
 * {@link MeasurementOrFactTable}.
 *
 * @see <a href="http://rs.gbif.org/extension/dwc/measurements_or_facts.xml</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MeasurementOrFactTableConverter {

  private static final Function<MeasurementOrFactTable, TargetHandler<MeasurementOrFactTable>>
      VERBATIM_HANDLER =
          mft ->
              ExtensionInterpretation.extension(Extension.MEASUREMENT_OR_FACT)
                  .to(() -> mft)
                  // Verbatim
                  .map(DwcTerm.measurementID, MeasurementOrFactTable::setVMeasurementid)
                  .map(DwcTerm.measurementType, MeasurementOrFactTable::setVMeasurementtype)
                  .map(DwcTerm.measurementUnit, MeasurementOrFactTable::setVMeasurementunit)
                  .map(DwcTerm.measurementValue, MeasurementOrFactTable::setVMeasurementvalue)
                  .map(DwcTerm.measurementAccuracy, MeasurementOrFactTable::setVMeasurementaccuracy)
                  .map(
                      DwcTerm.measurementDeterminedBy,
                      MeasurementOrFactTable::setVMeasurementdeterminedby)
                  .map(
                      DwcTerm.measurementDeterminedDate,
                      MeasurementOrFactTable::setVMeasurementdetermineddate)
                  .map(DwcTerm.measurementMethod, MeasurementOrFactTable::setVMeasurementmethod)
                  .map(DwcTerm.measurementRemarks, MeasurementOrFactTable::setVMeasurementremarks);

  private static final Function<MeasurementOrFactTable, TargetHandler<MeasurementOrFactTable>>
      INTERPRETED_HANDLER =
          mft ->
              ExtensionInterpretation.extension(Extension.MEASUREMENT_OR_FACT)
                  .to(() -> mft)
                  .map(DwcTerm.measurementID, MeasurementOrFactTable::setMeasurementid)
                  .map(DwcTerm.measurementType, MeasurementOrFactTable::setMeasurementtype)
                  .map(DwcTerm.measurementUnit, MeasurementOrFactTable::setMeasurementunit)
                  .map(DwcTerm.measurementValue, MeasurementOrFactTable::setMeasurementvalue)
                  .map(DwcTerm.measurementAccuracy, MeasurementOrFactTable::setMeasurementaccuracy)
                  .map(
                      DwcTerm.measurementDeterminedBy,
                      MeasurementOrFactTable::setMeasurementdeterminedby)
                  .map(
                      DwcTerm.measurementDeterminedDate,
                      MeasurementOrFactTable::setMeasurementdetermineddate)
                  .map(DwcTerm.measurementMethod, MeasurementOrFactTable::setMeasurementmethod)
                  .map(DwcTerm.measurementRemarks, MeasurementOrFactTable::setMeasurementremarks);

  /**
   * Convert measurements or facts of a {@link ExtendedRecord} and populates a {@link
   * MeasurementOrFactTable} with the interpreted values.
   */
  public static Optional<MeasurementOrFactTable> convert(BasicRecord br, ExtendedRecord er) {
    if (er == null || br == null) {
      return Optional.empty();
    }
    MeasurementOrFactTable table =
        MeasurementOrFactTable.newBuilder().setGbifid(br.getGbifId()).build();

    return VERBATIM_HANDLER
        .apply(table)
        .convert(er)
        .get()
        .flatMap(x -> INTERPRETED_HANDLER.apply(x).convert(er).get());
  }
}
