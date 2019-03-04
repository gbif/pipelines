package org.gbif.pipelines.core.interpreters.extension;

import java.util.Objects;

import org.gbif.pipelines.core.ExtensionInterpretation;
import org.gbif.pipelines.core.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.io.avro.Amplification;
import org.gbif.pipelines.io.avro.AmplificationRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Interpreter for the Amplification extension, Interprets form {@link ExtendedRecord} to {@link AmplificationRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/ggbn/amplification.xml</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AmplificationInterpreter {

  public static final String EXTENSION_ROW_TYPE = "http://rs.gbif.org/extension/ggbn/amplification.xml";

  private static final String GGBN = "http://data.ggbn.org/schemas/ggbn/terms/";
  private static final String GENSC = "http://gensc.org/ns/mixs/";

  private static final TargetHandler<Amplification> HANDLER =
      ExtensionInterpretation.extension(EXTENSION_ROW_TYPE)
          .to(Amplification::new)
          .map(GGBN + "amplificationDate", Amplification::setAmplificationDate)
          .map(GGBN + "amplificationStaff", Amplification::setAmplificationStaff)
          .map(GGBN + "amplificationSuccess", Amplification::setAmplificationSuccess)
          .map(GGBN + "amplificationSuccessDetails", Amplification::setAmplificationSuccessDetails)
          .map(GGBN + "amplificationMethod", Amplification::setAmplificationMethod)
          .map(GGBN + "primerSequenceForward", Amplification::setPrimerSequenceForward)
          .map(GGBN + "primerNameForward", Amplification::setPrimerNameForward)
          .map(GGBN + "primerReferenceCitationForward", Amplification::setPrimerReferenceCitationForward)
          .map(GGBN + "primerReferenceLinkForward", Amplification::setPrimerReferenceLinkForward)
          .map(GGBN + "primerSequenceReverse", Amplification::setPrimerSequenceReverse)
          .map(GGBN + "primerNameReverse", Amplification::setPrimerNameReverse)
          .map(GGBN + "primerReferenceCitationReverse", Amplification::setPrimerReferenceCitationReverse)
          .map(GGBN + "primerReferenceLinkReverse", Amplification::setPrimerReferenceLinkReverse)
          .map(GGBN + "purificationMethod", Amplification::setPurificationMethod)
          .map(GGBN + "consensusSequence", Amplification::setConsensusSequence)
          .map(GGBN + "consensusSequenceLength", Amplification::setConsensusSequenceLength)
          .map(GGBN + "consensusSequenceChromatogramFileURI", Amplification::setConsensusSequenceChromatogramFileUri)
          .map(GGBN + "barcodeSequence", Amplification::setBarcodeSequence)
          .map(GGBN + "haplotype", Amplification::setHaplotype)
          .map(GGBN + "marker", Amplification::setMarker)
          .map(GGBN + "markerSubfragment", Amplification::setMarkerSubfragment)
          .map(GGBN + "geneticAccessionNumber", Amplification::setGeneticAccessionNumber)
          .map(GGBN + "BOLDProcessID", Amplification::setBoldProcessId)
          .map(GGBN + "geneticAccessionURI", Amplification::setGeneticAccessionUri)
          .map(GGBN + "GC-content", Amplification::setGcContent)
          .map(GGBN + "markerAccordance", Amplification::setMarkerAccordance)
          .map(GENSC + "chimera_check", Amplification::setChimeraCheck)
          .map(GENSC + "assembly", Amplification::setAssembly)
          .map(GENSC + "sop", Amplification::setSop)
          .map(GENSC + "finishing_strategy", Amplification::setFinishingStrategy)
          .map(GENSC + "annot_source", Amplification::setAnnotSource)
          .map(GENSC + "seq_quality_check", Amplification::setSeqQualityCheck)
          .map(GENSC + "adapters", Amplification::setAdapters)
          .map(GENSC + "mid", Amplification::setMid);

  /**
   * Interprets amplifications of a {@link ExtendedRecord} and populates a {@link AmplificationRecord}
   * with the interpreted values.
   */
  public static void interpret(ExtendedRecord er, AmplificationRecord ar) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(ar);

    Result<Amplification> result = HANDLER.convert(er);

    ar.setAmplificationItems(result.getList());
    ar.getIssues().setIssueList(result.getIssuesAsList());
  }

}
