package org.gbif.pipelines.interpretation;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.common.parsers.BasisOfRecordParser;
import org.gbif.common.parsers.EstablishmentMeansParser;
import org.gbif.common.parsers.LifeStageParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.SexParser;
import org.gbif.common.parsers.TypeStatusParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.Validation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;

import static org.gbif.pipelines.interpretation.InterpretationTupleTags.interpretedExtendedRecordTupleTag;
import static org.gbif.pipelines.interpretation.InterpretationTupleTags.occurrenceIssueTupleTag;

/**
 * This transformation provides interpretation for the record level fields: basisOfRecord, sex,
 * individualCount, establishmentMeans and lifeStage.
 */
public class ExtendedRecordTransform extends PTransform<PCollection<ExtendedRecord>, PCollectionTuple> {


  private static final TypeStatusParser TYPE_PARSER = TypeStatusParser.getInstance();
  private static final BasisOfRecordParser BOR_PARSER = BasisOfRecordParser.getInstance();
  private static final SexParser SEX_PARSER = SexParser.getInstance();
  private static final EstablishmentMeansParser EST_PARSER = EstablishmentMeansParser.getInstance();
  private static final LifeStageParser LST_PARSER = LifeStageParser.getInstance();

  @Override
  public PCollectionTuple expand(PCollection<ExtendedRecord> input) {
    return input.apply("Interpreting record level terms", ParDo.of(interprete())
      .withOutputTags(interpretedExtendedRecordTupleTag(), TupleTagList.of(occurrenceIssueTupleTag())));
  }

  /**
   * Transforms a ExtendedRecord into a InterpretedExtendedRecord.
   */
  private DoFn<ExtendedRecord,InterpretedExtendedRecord> interprete() {
    return new DoFn<ExtendedRecord, InterpretedExtendedRecord>() {
      @ProcessElement
      public void processElement(ProcessContext context) {
        InterpretedExtendedRecord interpretedExtendedRecord = new InterpretedExtendedRecord();
        ExtendedRecord extendedRecord = context.element();
        interpretedExtendedRecord.setId(extendedRecord.getId());
        Collection<Validation> validations = new ArrayList<>();
        Optional.ofNullable(extendedRecord.getCoreTerms().get(DwcTerm.basisOfRecord.qualifiedName()))
          .ifPresent(value -> {
            ParseResult<BasisOfRecord> parseResult = BOR_PARSER.parse(value.toString());
            if (parseResult.isSuccessful()) {
              interpretedExtendedRecord.setBasisOfRecord(parseResult.getPayload().name());
            } else {
              validations.add(toValidation(OccurrenceIssue.BASIS_OF_RECORD_INVALID));
            }
          });

        Optional.ofNullable(extendedRecord.getCoreTerms().get(DwcTerm.sex.qualifiedName()))
          .ifPresent(value -> {
            ParseResult<Sex> parseResult = SEX_PARSER.parse(value.toString());
            if (parseResult.isSuccessful()) {
              interpretedExtendedRecord.setSex(parseResult.getPayload().name());
            }
          });

        Optional.ofNullable(extendedRecord.getCoreTerms().get(DwcTerm.establishmentMeans.qualifiedName()))
          .ifPresent(value -> {
            ParseResult<EstablishmentMeans> parseResult = EST_PARSER.parse(value.toString());
            if (parseResult.isSuccessful()) {
              interpretedExtendedRecord.setEstablishmentMeans(parseResult.getPayload().name());
            }
          });

        Optional.ofNullable(extendedRecord.getCoreTerms().get(DwcTerm.lifeStage.qualifiedName()))
          .ifPresent(value -> {
            ParseResult<LifeStage> parseResult = LST_PARSER.parse(value.toString());
            if (parseResult.isSuccessful()) {
              interpretedExtendedRecord.setLifeStage(parseResult.getPayload().name());
            }
          });

        Optional.ofNullable(extendedRecord.getCoreTerms().get(DwcTerm.typeStatus.qualifiedName()))
          .ifPresent(value -> {
            ParseResult<TypeStatus> parseResult = TYPE_PARSER.parse(value.toString());
            if (parseResult.isSuccessful()) {
              interpretedExtendedRecord.setTypeStatus(parseResult.getPayload().name());
            } else {
              validations.add(toValidation(OccurrenceIssue.TYPE_STATUS_INVALID));
            }
          });

        Optional.ofNullable(extendedRecord.getCoreTerms().get(DwcTerm.individualCount.qualifiedName()))
          .ifPresent(value -> {
            Optional<Integer> parseResult = Optional.ofNullable(NumberParser.parseInteger(value.toString()));
            if(parseResult.isPresent()) {
               interpretedExtendedRecord.setIndividualCount(parseResult.get());
            } else {
              validations.add(toValidation(OccurrenceIssue.INDIVIDUAL_COUNT_INVALID));
            }
          });

        if (!validations.isEmpty()) {
          context.output(occurrenceIssueTupleTag(), org.gbif.pipelines.io.avro.OccurrenceIssue.newBuilder()
            .setId(extendedRecord.getId())
            .setIssues(validations).build());
        }
        context.output(interpretedExtendedRecordTupleTag(), interpretedExtendedRecord);
      }
    };
  }

  /**
   * Translates a OccurrenceIssue into Validation object.
   */
  private static Validation toValidation(OccurrenceIssue occurrenceIssue) {
    return Validation.newBuilder()
      .setName(occurrenceIssue.name())
      .setSeverity(occurrenceIssue.getSeverity().name()).build();
  }
}
