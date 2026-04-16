package org.gbif.pipelines.core.interpreters.extension;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.Strings;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dna.core.SequenceProcessor;
import org.gbif.dwc.terms.GbifDnaTerm;
import org.gbif.dwc.terms.MixsTerm;
import org.gbif.pipelines.core.config.model.DnaConfig;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.core.interpreters.core.VocabularyInterpreter;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.io.avro.DnaDerivedData;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

@Builder(buildMethodName = "create")
@Slf4j
public class DnaDerivedDataInterpreter {

  private final VocabularyService vocabularyService;
  private final DnaConfig dnaConfig;

  /**
   * Interprets DNA data of a {@link ExtendedRecord} and populates a {@link DnaDerivedDataRecord}
   * with the interpreted values.
   */
  public void interpret(ExtendedRecord er, DnaDerivedDataRecord dr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(dr);

    ExtensionInterpretation.Result<DnaDerivedData> result =
        ExtensionInterpretation.extension(Extension.DNA_DERIVED_DATA)
            .to(DnaDerivedData::new)
            .map(GbifDnaTerm.dna_sequence, this::interpretSequence)
            .map(MixsTerm.target_gene, this::interpretTargetGene)
            .convert(er);

    dr.setDnaDerivedDataItems(result.getList());
  }

  private List<String> interpretTargetGene(DnaDerivedData dnaDerivedData, String rawValue) {
    List<String> issues = new ArrayList<>();
    VocabularyInterpreter.interpretVocabulary(
            MixsTerm.target_gene,
            rawValue,
            vocabularyService,
            v -> issues.add(OccurrenceIssue.TARGET_GENE_INVALID.name()))
        .ifPresent(dnaDerivedData::setTargetGene);

    return issues;
  }

  private List<String> interpretSequence(DnaDerivedData dnaDerivedData, String rawValue) {
    List<String> issues = new ArrayList<>();
    if (!Strings.isNullOrEmpty(rawValue)) {
      SequenceProcessor sequenceProcessor = new SequenceProcessor();
      SequenceProcessor.Result result = sequenceProcessor.processOneSequence(rawValue);

      dnaDerivedData.setRawSequence(rawValue);
      dnaDerivedData.setNucleotideSequenceID(result.nucleotideSequenceID());
      dnaDerivedData.setSequence(result.sequence());
      dnaDerivedData.setSequenceLength(result.sequenceLength());
      dnaDerivedData.setGcContent(result.gcContent());
      dnaDerivedData.setNonIupacFraction(result.nonIupacFraction());
      dnaDerivedData.setNonACGTNFraction(result.nonACGTNFraction());
      dnaDerivedData.setNFraction(result.nFraction());
      dnaDerivedData.setNRunsCapped(result.nNrunsCapped());
      dnaDerivedData.setNaturalLanguageDetected(result.naturalLanguageDetected());
      dnaDerivedData.setEndsTrimmed(result.endsTrimmed());
      dnaDerivedData.setGapsOrWhitespaceRemoved(result.gapsOrWhitespaceRemoved());
      dnaDerivedData.setInvalid(result.invalid());

      if (result.naturalLanguageDetected()) {
        issues.add(OccurrenceIssue.NUCLEOTIDE_SEQUENCE_NATURAL_LANGUAGE.name());
      }

      if (result.endsTrimmed()) {
        issues.add(OccurrenceIssue.NUCLEOTIDE_SEQUENCE_ENDS_TRIMMED.name());
      }

      if (result.gapsOrWhitespaceRemoved()) {
        issues.add(OccurrenceIssue.NUCLEOTIDE_SEQUENCE_GAPS_REMOVED.name());
      }

      if (result.invalid()) {
        issues.add(OccurrenceIssue.NUCLEOTIDE_SEQUENCE_INVALID.name());
      }

      if (result.nFraction() > dnaConfig.getNucleotideSequenceHighNFractionThreshold()) {
        issues.add(OccurrenceIssue.NUCLEOTIDE_SEQUENCE_HIGH_N_FRACTION.name());
      }

      if (result.nonACGTNFraction() > dnaConfig.getNucleotideSequenceHighAmbiguityThreshold()) {
        issues.add(OccurrenceIssue.NUCLEOTIDE_SEQUENCE_HIGH_AMBIGUITY.name());
      }
    }

    return issues;
  }
}
