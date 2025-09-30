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
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Humboldt;
import org.gbif.pipelines.io.avro.HumboldtRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class HumboldtTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static class CleanDateCreate extends DoFn<HumboldtRecord, HumboldtRecord> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      HumboldtRecord record = HumboldtRecord.newBuilder(context.element()).build();
      record.setCreated(null);
      context.output(record);
    }
  }

  private static final String RECORD_ID = "777";

  @Test
  public void transformationExtAndDynamicTest() {

    // Expected
    Humboldt h1 = Humboldt.newBuilder().setSiteCount(2).build();
    Humboldt h2 = Humboldt.newBuilder().setVerbatimSiteNames(List.of("sn")).build();

    List<Humboldt> humboldtList = Arrays.asList(h1, h2);
    HumboldtRecord hRec =
        HumboldtRecord.newBuilder().setId(RECORD_ID).setHumboldtItems(humboldtList).build();
    List<HumboldtRecord> result = Collections.singletonList(hRec);

    // State
    ExtendedRecord.Builder builder = ExtendedRecord.newBuilder().setId(RECORD_ID);

    Map<String, String> map1 = new HashMap<>();
    map1.put(EcoTerm.siteCount.qualifiedName(), "2");
    Map<String, String> map2 = new HashMap<>();
    map2.put(EcoTerm.verbatimSiteNames.qualifiedName(), "sn ");

    builder.setExtensions(
        Collections.singletonMap(Extension.HUMBOLDT.getRowType(), Arrays.asList(map1, map2)));
    ExtendedRecord extendedRecord = builder.build();

    // When
    PCollection<HumboldtRecord> dataStream =
        p.apply(Create.of(extendedRecord))
            .apply(
                HumboldtTransform.builder()
                    .vocabularyServiceSupplier(() -> null)
                    .nameUsageMatchKvStoreSupplier(() -> null)
                    .create()
                    .interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).containsInAnyOrder(result);
    p.run();
  }
}
