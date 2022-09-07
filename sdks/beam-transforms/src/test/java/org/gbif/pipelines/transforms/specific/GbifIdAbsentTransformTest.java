package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_INVALID;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;

import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.keygen.HBaseLockingKey;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class GbifIdAbsentTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

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
    public Optional<KeyLookupResult> findKey(Set<String> uniqueStrings, String scope) {
      return Optional.of(generateKey(uniqueStrings, scope));
    }

    @Override
    public Optional<KeyLookupResult> findKey(Set<String> uniqueStrings) {
      return Optional.of(generateKey(uniqueStrings, scope));
    }

    @Override
    public void close() {}
  }

  @Test
  public void invalidIssueTest() {

    // State
    String id = "777";
    IdentifierRecord inIr = IdentifierRecord.newBuilder().setId(id).build();

    // Expected
    IdentifierRecord outIr = IdentifierRecord.newBuilder().setId(id).build();
    addIssue(outIr, GBIF_ID_INVALID);

    // When
    GbifIdAbsentTransform gbifIdTransform =
        GbifIdAbsentTransform.builder()
            .keygenServiceSupplier(() -> HBaseLockingKeyServiceStub.create(id))
            .create();

    PCollection<IdentifierRecord> girCollection =
        p.apply("Read IdentifierRecord", Create.of(inIr))
            .apply("Interpret IDs", gbifIdTransform.interpret());

    // Should
    PAssert.that(girCollection).containsInAnyOrder(outIr);
    p.run();
  }

  @Test
  public void gbifIdTest() {

    // State
    String id = "777";
    IdentifierRecord inGir =
        IdentifierRecord.newBuilder().setId(id).setUniqueKey("occ").setAssociatedKey("tr").build();

    // Expected
    IdentifierRecord outGir =
        IdentifierRecord.newBuilder(inGir)
            .setInternalId(id)
            .setUniqueKey("occ")
            .setAssociatedKey("tr")
            .build();

    // When
    GbifIdAbsentTransform gbifIdTransform =
        GbifIdAbsentTransform.builder()
            .isOccurrenceIdValid(true)
            .keygenServiceSupplier(() -> HBaseLockingKeyServiceStub.create(id))
            .create();

    PCollection<IdentifierRecord> girCollection =
        p.apply("Read IdentifierRecord", Create.of(inGir))
            .apply("Interpret IDs", gbifIdTransform.interpret());

    // Should
    PAssert.that(girCollection).containsInAnyOrder(outGir);
    p.run();
  }
}
