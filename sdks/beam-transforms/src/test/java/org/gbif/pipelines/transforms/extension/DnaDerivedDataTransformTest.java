package org.gbif.pipelines.transforms.extension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.GbifDnaTerm;
import org.gbif.pipelines.io.avro.DnaDerivedData;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class DnaDerivedDataTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static class CleanDateCreate extends DoFn<DnaDerivedDataRecord, DnaDerivedDataRecord> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      DnaDerivedDataRecord record = DnaDerivedDataRecord.newBuilder(context.element()).build();
      record.setCreated(null);
      context.output(record);
    }
  }

  private static final String RECORD_ID = "777";

  @Test
  public void transformationExtAndDynamicTest() {

    // Expected
    DnaDerivedData dna1 =
        DnaDerivedData.newBuilder().setDnaSequenceID("a6bf0d942fd67916f50079e5c44a41b0").build();
    DnaDerivedData dna2 =
        DnaDerivedData.newBuilder().setDnaSequenceID("1f2bf69551598784ff845c795c021e8d").build();

    List<DnaDerivedData> dnaList = Arrays.asList(dna1, dna2);
    DnaDerivedDataRecord dnaRec =
        DnaDerivedDataRecord.newBuilder().setId(RECORD_ID).setDnaDerivedDataItems(dnaList).build();
    List<DnaDerivedDataRecord> result = Collections.singletonList(dnaRec);

    // State
    ExtendedRecord.Builder builder = ExtendedRecord.newBuilder().setId(RECORD_ID);

    Map<String, String> map1 = new HashMap<>();
    map1.put(GbifDnaTerm.dna_sequence.qualifiedName(), "ccacacct");
    Map<String, String> map2 = new HashMap<>();
    map2.put(GbifDnaTerm.dna_sequence.qualifiedName(), "aaaacacct");

    builder.setExtensions(
        Collections.singletonMap(
            Extension.DNA_DERIVED_DATA.getRowType(), Arrays.asList(map1, map2)));
    ExtendedRecord extendedRecord = builder.build();

    // When
    PCollection<DnaDerivedDataRecord> dataStream =
        p.apply(Create.of(extendedRecord))
            .apply(DnaDerivedDataTransform.builder().create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).containsInAnyOrder(result);
    p.run();
  }
}
