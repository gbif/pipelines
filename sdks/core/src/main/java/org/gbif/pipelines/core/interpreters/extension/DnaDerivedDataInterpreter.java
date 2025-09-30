package org.gbif.pipelines.core.interpreters.extension;

import java.util.Objects;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.Strings;
import org.gbif.api.util.DnaUtils;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.GbifDnaTerm;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.io.avro.DnaDerivedData;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@Builder(buildMethodName = "create")
@Slf4j
public class DnaDerivedDataInterpreter {

  /**
   * Interprets audubon of a {@link ExtendedRecord} and populates a {@link DnaDerivedDataRecord}
   * with the interpreted values.
   */
  public void interpret(ExtendedRecord er, DnaDerivedDataRecord dr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(dr);

    ExtensionInterpretation.Result<DnaDerivedData> result =
        ExtensionInterpretation.extension(Extension.DNA_DERIVED_DATA)
            .to(DnaDerivedData::new)
            .map(GbifDnaTerm.dna_sequence, DnaDerivedDataInterpreter::interpretDnaSequence)
            .convert(er);

    dr.setDnaDerivedDataItems(result.getList());
  }

  private static void interpretDnaSequence(DnaDerivedData dnaDerivedData, String rawValue) {
    if (!Strings.isNullOrEmpty(rawValue)) {
      dnaDerivedData.setDnaSequenceID(DnaUtils.convertDnaSequenceToID(rawValue));
    }
  }
}
