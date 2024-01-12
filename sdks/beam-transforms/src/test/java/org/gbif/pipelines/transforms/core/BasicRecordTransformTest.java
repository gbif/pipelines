package org.gbif.pipelines.transforms.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.BASIS_OF_RECORD_INVALID;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
import org.gbif.pipelines.io.avro.GeologicalContext;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.VocabularyConcept;
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
      "0", "OBSERVATION", "MALE", "INTRODUCED", "HOLOTYPE", "2", "http://refs.com"
    };
    final String[] two = {
      "1", "OCCURRENCE", "HERMAPHRODITE", "INTRODUCED", "HAPANTOTYPE", "1", "http://refs.com"
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
            .setBasisOfRecord(BasisOfRecord.OCCURRENCE.name())
            .setCreated(0L)
            .setLicense(License.UNSPECIFIED.name())
            .setIsSequenced(Boolean.TRUE)
            .setAssociatedSequences(Collections.singletonList("dawd"))
            .setIssues(
                IssueRecord.newBuilder()
                    .setIssueList(Collections.singletonList(BASIS_OF_RECORD_INVALID.name()))
                    .build())
            .build();

    Map<String, String> map = new HashMap<>(2);
    map.put(DwcTerm.sex.qualifiedName(), "");
    map.put(DwcTerm.associatedSequences.qualifiedName(), "dawd");

    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").setCoreTerms(map).build();

    PCollection<BasicRecord> recordCollection =
        p.apply(Create.of(er))
            .apply(BasicTransform.builder().create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void geologicalContextTest() {

    Function<String, VocabularyConcept> vcFn =
        v ->
            VocabularyConcept.newBuilder()
                .setConcept(v)
                .setLineage(Collections.singletonList(v))
                .build();

    // Expected
    BasicRecord expected =
        BasicRecord.newBuilder()
            .setId("777")
            .setBasisOfRecord(BasisOfRecord.OCCURRENCE.name())
            .setCreated(0L)
            .setLicense(License.UNSPECIFIED.name())
            .setIsSequenced(Boolean.FALSE)
            .setIssues(
                IssueRecord.newBuilder()
                    .setIssueList(Collections.singletonList(BASIS_OF_RECORD_INVALID.name()))
                    .build())
            .setGeologicalContext(
                GeologicalContext.newBuilder()
                    .setEarliestEonOrLowestEonothem(vcFn.apply("test1"))
                    .setLatestEonOrHighestEonothem(vcFn.apply("test2"))
                    .setEarliestEraOrLowestErathem(vcFn.apply("test3"))
                    .setLatestEraOrHighestErathem(vcFn.apply("test4"))
                    .setEarliestPeriodOrLowestSystem(vcFn.apply("test5"))
                    .setLatestPeriodOrHighestSystem(vcFn.apply("test6"))
                    .setEarliestEpochOrLowestSeries(vcFn.apply("test7"))
                    .setLatestEpochOrHighestSeries(vcFn.apply("test8"))
                    .setEarliestAgeOrLowestStage(vcFn.apply("test9"))
                    .setLatestAgeOrHighestStage(vcFn.apply("test10"))
                    .setLowestBiostratigraphicZone("test11")
                    .setHighestBiostratigraphicZone("test12")
                    .setGroup("test13")
                    .setFormation("test14")
                    .setMember("test15")
                    .setBed("test16")
                    .build())
            .build();

    // State
    Map<String, String> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.earliestEonOrLowestEonothem.qualifiedName(), "test1");
    coreTerms.put(DwcTerm.latestEonOrHighestEonothem.qualifiedName(), "test2");
    coreTerms.put(DwcTerm.earliestEraOrLowestErathem.qualifiedName(), "test3");
    coreTerms.put(DwcTerm.latestEraOrHighestErathem.qualifiedName(), "test4");
    coreTerms.put(DwcTerm.earliestPeriodOrLowestSystem.qualifiedName(), "test5");
    coreTerms.put(DwcTerm.latestPeriodOrHighestSystem.qualifiedName(), "test6");
    coreTerms.put(DwcTerm.earliestEpochOrLowestSeries.qualifiedName(), "test7");
    coreTerms.put(DwcTerm.latestEpochOrHighestSeries.qualifiedName(), "test8");
    coreTerms.put(DwcTerm.earliestAgeOrLowestStage.qualifiedName(), "test9");
    coreTerms.put(DwcTerm.latestAgeOrHighestStage.qualifiedName(), "test10");
    coreTerms.put(DwcTerm.lowestBiostratigraphicZone.qualifiedName(), "test11");
    coreTerms.put(DwcTerm.highestBiostratigraphicZone.qualifiedName(), "test12");
    coreTerms.put(DwcTerm.group.qualifiedName(), "test13");
    coreTerms.put(DwcTerm.formation.qualifiedName(), "test14");
    coreTerms.put(DwcTerm.member.qualifiedName(), "test15");
    coreTerms.put(DwcTerm.bed.qualifiedName(), "test16");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").setCoreTerms(coreTerms).build();

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
              record.getCoreTerms().put(DwcTerm.typeStatus.qualifiedName(), x[4]);
              record.getCoreTerms().put(DwcTerm.individualCount.qualifiedName(), x[5]);
              record.getCoreTerms().put(DcTerm.references.qualifiedName(), x[6]);
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
                    .setTypeStatus(Collections.singletonList(x[4]))
                    .setIndividualCount(Integer.valueOf(x[5]))
                    .setReferences(x[6])
                    .setIsSequenced(Boolean.FALSE)
                    .setLicense(License.UNSPECIFIED.name())
                    .build())
        .collect(Collectors.toList());
  }
}
