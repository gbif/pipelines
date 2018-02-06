package org.gbif.pipelines.interpretation;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.interpretation.parsers.SimpleTypeParser;
import org.gbif.pipelines.interpretation.parsers.VocabularyParsers;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.Validation;

import java.util.ArrayList;
import java.util.Collection;

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

  @Override
  public PCollectionTuple expand(PCollection<ExtendedRecord> input) {
    return input.apply("Interpreting record level terms", ParDo.of(interpret())
      .withOutputTags(interpretedExtendedRecordTupleTag(), TupleTagList.of(occurrenceIssueTupleTag())));
  }

  /**
   * Transforms a ExtendedRecord into a InterpretedExtendedRecord.
   */
  private DoFn<ExtendedRecord,InterpretedExtendedRecord> interpret() {
    return new DoFn<ExtendedRecord, InterpretedExtendedRecord>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        //Transformation main output
        InterpretedExtendedRecord interpretedExtendedRecord = new InterpretedExtendedRecord();

        //Context element to be interpreted
        ExtendedRecord extendedRecord = context.element();

        //Id
        interpretedExtendedRecord.setId(extendedRecord.getId());
        Collection<Validation> validations = new ArrayList<>();

        //Interpretation routines
        interpretBasisOfRecord(interpretedExtendedRecord, extendedRecord, validations);

        interpretSex(interpretedExtendedRecord, extendedRecord);

        interpretEstablishmentMeans(interpretedExtendedRecord, extendedRecord);

        interpretLifeStage(interpretedExtendedRecord, extendedRecord);

        interpretTypeStatus(interpretedExtendedRecord, extendedRecord, validations);

        interpretIndividualCount(interpretedExtendedRecord, extendedRecord, validations);

        //additional outputs
        if (!validations.isEmpty()) {
          context.output(occurrenceIssueTupleTag(), org.gbif.pipelines.io.avro.OccurrenceIssue.newBuilder()
            .setId(extendedRecord.getId())
            .setIssues(validations).build());
        }

        //main output
        context.output(interpretedExtendedRecordTupleTag(), interpretedExtendedRecord);
      }

      /**
       * {@link DwcTerm#individualCount} interpretation.
       */
      private void interpretIndividualCount(InterpretedExtendedRecord interpretedExtendedRecord,
                                            ExtendedRecord extendedRecord, Collection<Validation> validations) {
        SimpleTypeParser.parseInt(extendedRecord, DwcTerm.individualCount, parseResult -> {
            if(parseResult.isPresent()) {
               interpretedExtendedRecord.setIndividualCount(parseResult.get());
            } else {
              validations.add(toValidation(OccurrenceIssue.INDIVIDUAL_COUNT_INVALID));
            }
          });
      }

      /**
       * {@link DwcTerm#typeStatus} interpretation.
       */
      private void interpretTypeStatus(InterpretedExtendedRecord interpretedExtendedRecord, ExtendedRecord extendedRecord,
                                       Collection<Validation> validations) {
        VocabularyParsers
          .typeStatusParser()
          .parse(extendedRecord, parseResult -> {
            if (parseResult.isSuccessful()) {
              interpretedExtendedRecord.setTypeStatus(parseResult.getPayload().name());
            } else {
              validations.add(toValidation(OccurrenceIssue.TYPE_STATUS_INVALID));
            }
          });
      }

      /**
       * {@link DwcTerm#lifeStage} interpretation.
       */
      private void interpretLifeStage(InterpretedExtendedRecord interpretedExtendedRecord,
                                      ExtendedRecord extendedRecord) {
        VocabularyParsers
          .lifeStageParser()
          .parse(extendedRecord, parseResult -> {
            if (parseResult.isSuccessful()) {
              interpretedExtendedRecord.setLifeStage(parseResult.getPayload().name());
            }
          });
      }

      /**
       * {@link DwcTerm#establishmentMeans} interpretation.
       */
      private void interpretEstablishmentMeans(InterpretedExtendedRecord interpretedExtendedRecord,
                                               ExtendedRecord extendedRecord) {
        VocabularyParsers
          .establishmentMeansParser()
          .parse(extendedRecord, parseResult -> {
            if (parseResult.isSuccessful()) {
              interpretedExtendedRecord.setEstablishmentMeans(parseResult.getPayload().name());
            }
          });
      }

      /**
       * {@link DwcTerm#sex} interpretation.
       */
      private void interpretSex(InterpretedExtendedRecord interpretedExtendedRecord, ExtendedRecord extendedRecord) {
        VocabularyParsers
          .sexParser()
          .parse(extendedRecord, parseResult -> {
            if (parseResult.isSuccessful()) {
              interpretedExtendedRecord.setSex(parseResult.getPayload().name());
            }});
      }

      /**
       * {@link DwcTerm#basisOfRecord} interpretation.
       */
      private void interpretBasisOfRecord(InterpretedExtendedRecord interpretedExtendedRecord,
                                          ExtendedRecord extendedRecord, Collection<Validation> validations) {
        VocabularyParsers
          .basisOfRecordParser()
          .parse(extendedRecord, parseResult -> {
            if (parseResult.isSuccessful()) {
              interpretedExtendedRecord.setBasisOfRecord(parseResult.getPayload().name());
            } else {
              validations.add(toValidation(OccurrenceIssue.BASIS_OF_RECORD_INVALID));
            }
          });
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
