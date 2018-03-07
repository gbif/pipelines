package org.gbif.pipelines.transform.record;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.transform.Kv2Value;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class InterpretedExtendedRecordTransformTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testTransformation() {

    // State
    final String[] one = {"0", "OBSERVATION", "MALE", "INTRODUCED", "SPOROPHYTE", "HOLOTYPE", "2"};
    final String[] two = {"1", "UNKNOWN", "HERMAPHRODITE", "INTRODUCED", "GAMETE", "HAPANTOTYPE", "1"};
    final List<ExtendedRecord> records = createExtendedRecordList(one, two);

    // Expected
    final List<InterpretedExtendedRecord> interpretedRecords = createInterpretedExtendedRecordList(one, two);

    // When
    InterpretedExtendedRecordTransform interpretedTransform = new InterpretedExtendedRecordTransform().withAvroCoders(p);

    PCollection<ExtendedRecord> inputStream = p.apply(Create.of(records));

    PCollectionTuple tuple = inputStream.apply(interpretedTransform);

    PCollection<InterpretedExtendedRecord> recordCollection = tuple.get(interpretedTransform.getDataTag())
      .apply(Kv2Value.create());

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(interpretedRecords);
    p.run();

  }

  private List<ExtendedRecord> createExtendedRecordList(String[]... records) {
    return Arrays.stream(records).map(x -> {
      ExtendedRecord record = ExtendedRecord.newBuilder().setId(x[0]).build();
      record.getCoreTerms().put(DwcTerm.basisOfRecord.qualifiedName(), x[1]);
      record.getCoreTerms().put(DwcTerm.sex.qualifiedName(), x[2]);
      record.getCoreTerms().put(DwcTerm.establishmentMeans.qualifiedName(), x[3]);
      record.getCoreTerms().put(DwcTerm.lifeStage.qualifiedName(), x[4]);
      record.getCoreTerms().put(DwcTerm.typeStatus.qualifiedName(), x[5]);
      record.getCoreTerms().put(DwcTerm.individualCount.qualifiedName(), x[6]);
      return record;
    }).collect(Collectors.toList());
  }

  private List<InterpretedExtendedRecord> createInterpretedExtendedRecordList(String[]... records) {
    return Arrays.stream(records)
      .map(x -> InterpretedExtendedRecord.newBuilder()
        .setId(x[0])
        .setBasisOfRecord(x[1])
        .setSex(x[2])
        .setEstablishmentMeans(x[3])
        .setLifeStage(x[4])
        .setTypeStatus(x[5])
        .setIndividualCount(Integer.valueOf(x[6]))
        .build())
      .collect(Collectors.toList());
  }

}
