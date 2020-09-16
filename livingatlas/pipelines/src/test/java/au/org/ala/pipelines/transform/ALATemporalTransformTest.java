package au.org.ala.pipelines.transform;

import au.org.ala.pipelines.transforms.ALATemporalTransform;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.parsers.config.model.PipelinesConfig;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ALATemporalTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private PipelinesConfig config;

  @Before
  public void set() {
    config = new PipelinesConfig();
    config.setDefaultDateFormat("DMY");
  }

  private static class CleanDateCreate extends DoFn<TemporalRecord, TemporalRecord> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      TemporalRecord tr = TemporalRecord.newBuilder(context.element()).build();
      tr.setCreated(0L);
      context.output(tr);
    }
  }

  @Test
  public void DMYtransformationTest_on_GBIF() {
    // State
    final List<ExtendedRecord> input = new ArrayList<>();

    ExtendedRecord record = ExtendedRecord.newBuilder().setId("0").build();
    record.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), "01/02/1999T12:26Z");
    record.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "01/04/1999");
    record.getCoreTerms().put(DcTerm.modified.qualifiedName(), "01/03/1999T12:26Z");
    input.add(record);
    // Expected
    TemporalRecord expected1 =
        TemporalRecord.newBuilder()
            .setId("0")
            .setEventDate(EventDate.newBuilder().setGte("1999-02-01T12:26").build())
            .setYear(1999)
            .setMonth(2)
            .setDay(1)
            .setDateIdentified("1999-04-01T00:00")
            .setModified("1999-03-01T12:26")
            .setCreated(0L)
            .build();

    final List<TemporalRecord> dataExpected = new ArrayList<>();

    dataExpected.add(expected1);

    // When
    PCollection<TemporalRecord> dataStream =
        p.apply(Create.of(input))
            .apply(TemporalTransform.create(config).interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).containsInAnyOrder(dataExpected);
    p.run();
  }

  /** Will raise ALA assertion */
  @Test
  public void DMYtransformationTest_on_ALA() {
    // State
    final List<ExtendedRecord> input = new ArrayList<>();

    ExtendedRecord record = ExtendedRecord.newBuilder().setId("0").build();
    record.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), "01/02/1999T12:26Z");
    record.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "01/04/1999");
    record.getCoreTerms().put(DcTerm.modified.qualifiedName(), "01/03/1999T12:26Z");
    input.add(record);
    // Expected
    TemporalRecord expected1 =
        TemporalRecord.newBuilder()
            .setId("0")
            .setEventDate(EventDate.newBuilder().setGte("1999-02-01T12:26").build())
            .setYear(1999)
            .setMonth(2)
            .setDay(1)
            .setDateIdentified("1999-04-01T00:00")
            .setModified("1999-03-01T12:26")
            .setCreated(0L)
            .setIssues(
                IssueRecord.newBuilder().setIssueList(Arrays.asList("FIRST_OF_MONTH")).build())
            .build();

    final List<TemporalRecord> dataExpected = new ArrayList<>();

    dataExpected.add(expected1);

    // When
    PCollection<TemporalRecord> dataStream =
        p.apply(Create.of(input))
            .apply(ALATemporalTransform.create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).containsInAnyOrder(dataExpected);
    p.run();
  }
}
