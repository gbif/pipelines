package org.gbif.pipelines.transforms.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.BASIS_OF_RECORD_INVALID;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.License;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class BasicRecordTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static class CleanDateCreate extends DoFn<BasicRecord, BasicRecord> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      BasicRecord br = BasicRecord.newBuilder(context.element()).build();
      br.setCreated(0L);
      context.output(br);
    }
  }

  @Test
  public void transformationTest() {

    // State
    final String[] one = {
      "0", "OBSERVATION", "MALE", "INTRODUCED", "SPOROPHYTE", "HOLOTYPE", "2", "http://refs.com"
    };
    final String[] two = {
      "1", "UNKNOWN", "HERMAPHRODITE", "INTRODUCED", "GAMETE", "HAPANTOTYPE", "1", "http://refs.com"
    };
    final List<ExtendedRecord> records = createExtendedRecordList(one, two);

    // Expected
    final List<BasicRecord> basicRecords = createBasicRecordList(one, two);

    // When
    PCollection<BasicRecord> recordCollection =
        p.apply(Create.of(records))
            .apply(BasicTransform.builder().create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(basicRecords);
    p.run();
  }

  @Test
  public void emptyErTest() {

    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").build();

    PCollection<BasicRecord> recordCollection =
        p.apply(Create.of(er))
            .apply(BasicTransform.builder().create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(recordCollection).empty();
    p.run();
  }

  @Test
  public void basisOfRecordTest() {
    // Expected
    BasicRecord expected =
        BasicRecord.newBuilder()
            .setId("777")
            .setBasisOfRecord(BasisOfRecord.UNKNOWN.name())
            .setCreated(0L)
            .setLicense(License.UNSPECIFIED.name())
            .setIssues(
                IssueRecord.newBuilder()
                    .setIssueList(Collections.singletonList(BASIS_OF_RECORD_INVALID.name()))
                    .build())
            .build();

    // State
    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId("777")
            .setCoreTerms(Collections.singletonMap(DwcTerm.sex.qualifiedName(), ""))
            .build();

    PCollection<BasicRecord> recordCollection =
        p.apply(Create.of(er))
            .apply(BasicTransform.builder().create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(expected);
    p.run();
  }

  private List<ExtendedRecord> createExtendedRecordList(String[]... records) {
    return Arrays.stream(records)
        .map(
            x -> {
              ExtendedRecord record = ExtendedRecord.newBuilder().setId(x[0]).build();
              record.getCoreTerms().put(DwcTerm.basisOfRecord.qualifiedName(), x[1]);
              record.getCoreTerms().put(DwcTerm.sex.qualifiedName(), x[2]);
              record.getCoreTerms().put(DwcTerm.establishmentMeans.qualifiedName(), x[3]);
              record.getCoreTerms().put(DwcTerm.lifeStage.qualifiedName(), x[4]);
              record.getCoreTerms().put(DwcTerm.typeStatus.qualifiedName(), x[5]);
              record.getCoreTerms().put(DwcTerm.individualCount.qualifiedName(), x[6]);
              record.getCoreTerms().put(DcTerm.references.qualifiedName(), x[7]);
              return record;
            })
        .collect(Collectors.toList());
  }

  private List<BasicRecord> createBasicRecordList(String[]... records) {
    return Arrays.stream(records)
        .map(
            x ->
                BasicRecord.newBuilder()
                    .setId(x[0])
                    .setCreated(0L)
                    .setBasisOfRecord(x[1])
                    .setSex(x[2])
                    .setEstablishmentMeans(x[3])
                    .setLifeStage(x[4])
                    .setTypeStatus(x[5])
                    .setIndividualCount(Integer.valueOf(x[6]))
                    .setReferences(x[7])
                    .setLicense(License.UNSPECIFIED.name())
                    .build())
        .collect(Collectors.toList());
  }
}
