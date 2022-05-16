package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_ABSENT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_INVALID;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;

import java.util.Collections;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class GbifIdTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static class CleanDateCreate extends DoFn<GbifIdRecord, GbifIdRecord> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      GbifIdRecord gir = GbifIdRecord.newBuilder(context.element()).build();
      gir.setCreated(null);
      context.output(gir);
    }
  }

  @AllArgsConstructor(staticName = "create")
  private static class HBaseLockingKeyServiceStub implements HBaseLockingKey {

    private String scope;

    @Override
    public KeyLookupResult generateKey(Set<String> uniqueStrings, String scope) {
      return new KeyLookupResult(Long.parseLong(scope), true);
    }

    @Override
    public KeyLookupResult generateKey(Set<String> uniqueStrings) {
      return generateKey(uniqueStrings, scope);
    }

    @Override
    public KeyLookupResult findKey(Set<String> uniqueStrings, String scope) {
      return generateKey(uniqueStrings, scope);
    }

    @Override
    public KeyLookupResult findKey(Set<String> uniqueStrings) {
      return generateKey(uniqueStrings, scope);
    }

    @Override
    public void close() {}
  }

  @Test
  public void useExtendedRecordIdTest() {

    // State
    String id = "777";
    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(id)
            .setCoreTerms(Collections.singletonMap("map", "value"))
            .build();

    // Expected
    GbifIdRecord gir = GbifIdRecord.newBuilder().setId(id).setGbifId(Long.valueOf(id)).build();

    // When
    GbifIdTransform gbifIdTransform = GbifIdTransform.builder().useExtendedRecordId(true).create();
    PCollection<GbifIdRecord> girCollection =
        p.apply("Read ExtendedRecord", Create.of(er))
            .apply("Interpret IDs", gbifIdTransform.interpret())
            .apply("Clean date", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(girCollection).containsInAnyOrder(gir);
    p.run();
  }

  @Test
  public void absentIssueTest() {

    // State
    String id = "777";
    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(id)
            .setCoreTerms(Collections.singletonMap("map", "value"))
            .build();

    // Expected
    GbifIdRecord gir = GbifIdRecord.newBuilder().setId(id).build();
    addIssue(gir, GBIF_ID_ABSENT);

    // When
    GbifIdTransform gbifIdTransform =
        GbifIdTransform.builder()
            .generateIdIfAbsent(false)
            .keygenServiceSupplier(() -> HBaseLockingKeyServiceStub.create(id))
            .create();

    PCollection<GbifIdRecord> girCollection =
        p.apply("Read ExtendedRecord", Create.of(er))
            .apply("Interpret IDs", gbifIdTransform.interpret())
            .apply("Clean date", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(girCollection).containsInAnyOrder(gir);
    p.run();
  }

  @Test
  public void invalidIssueTest() {

    // State
    String id = "777";
    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(id)
            .setCoreTerms(Collections.singletonMap("map", "value"))
            .build();

    // Expected
    GbifIdRecord gir = GbifIdRecord.newBuilder().setId(id).build();
    addIssue(gir, GBIF_ID_INVALID);

    // When
    GbifIdTransform gbifIdTransform =
        GbifIdTransform.builder()
            .generateIdIfAbsent(true)
            .keygenServiceSupplier(() -> HBaseLockingKeyServiceStub.create(id))
            .create();

    PCollection<GbifIdRecord> girCollection =
        p.apply("Read ExtendedRecord", Create.of(er))
            .apply("Interpret IDs", gbifIdTransform.interpret())
            .apply("Clean date", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(girCollection).containsInAnyOrder(gir);
    p.run();
  }

  @Test
  public void gbifIdTest() {

    // State
    String id = "777";
    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(id)
            .setCoreTerms(Collections.singletonMap(DwcTerm.occurrenceID.qualifiedName(), "value"))
            .build();

    // Expected
    GbifIdRecord gir =
        GbifIdRecord.newBuilder()
            .setId(id)
            .setGbifId(Long.valueOf(id))
            .setOccurrenceId("value")
            .build();

    // When
    GbifIdTransform gbifIdTransform =
        GbifIdTransform.builder()
            .generateIdIfAbsent(true)
            .isOccurrenceIdValid(true)
            .keygenServiceSupplier(() -> HBaseLockingKeyServiceStub.create(id))
            .create();

    PCollection<GbifIdRecord> girCollection =
        p.apply("Read ExtendedRecord", Create.of(er))
            .apply("Interpret IDs", gbifIdTransform.interpret())
            .apply("Clean date", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(girCollection).containsInAnyOrder(gir);
    p.run();
  }
}
