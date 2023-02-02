package org.gbif.pipelines.transforms.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class ExtensionFilterTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void nullSetBeamTest() {

    // State
    final List<ExtendedRecord> input =
        Collections.singletonList(
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    final List<ExtendedRecord> expected =
        Collections.singletonList(
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    // When
    PCollection<ExtendedRecord> result =
        p.apply(Create.of(input)).apply(ExtensionFilterTransform.create(null));

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void emptySetBeamTest() {

    // State
    final List<ExtendedRecord> input =
        Collections.singletonList(
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    final List<ExtendedRecord> expected =
        Collections.singletonList(
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    // When
    PCollection<ExtendedRecord> result =
        p.apply(Create.of(input)).apply(ExtensionFilterTransform.create(Collections.emptySet()));

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void allowExtensionBeamTest() {

    Map<String, List<Map<String, String>>> extMap = new HashMap<>();
    extMap.put("ext", Collections.singletonList(Collections.singletonMap("test", "test")));
    extMap.put("ext-empt", Collections.emptyList());

    // State
    final List<ExtendedRecord> input =
        Collections.singletonList(
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(extMap)
                .build());

    final List<ExtendedRecord> expected =
        Collections.singletonList(
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    final Set<String> allowSet = new HashSet<>(Arrays.asList("ext", "ext-empt"));

    // When
    PCollection<ExtendedRecord> result =
        p.apply(Create.of(input)).apply(ExtensionFilterTransform.create(allowSet));

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void excludeExtensionBeamTest() {

    // State
    final List<ExtendedRecord> input =
        Collections.singletonList(
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    final List<ExtendedRecord> expected =
        Collections.singletonList(
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(Collections.emptyMap())
                .build());

    final Set<String> allowSet = Collections.singleton("nop");

    // When
    PCollection<ExtendedRecord> result =
        p.apply(Create.of(input)).apply(ExtensionFilterTransform.create(allowSet));

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void nullSetJavaTest() {

    // State
    final Map<String, ExtendedRecord> input =
        Collections.singletonMap(
            "777",
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    final Map<String, ExtendedRecord> expected =
        Collections.singletonMap(
            "777",
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    // When
    Map<String, ExtendedRecord> result = ExtensionFilterTransform.create(null).transform(input);

    // Should
    assertMap(expected, result);
  }

  @Test
  public void emptySetJavaTest() {

    // State
    final Map<String, ExtendedRecord> input =
        Collections.singletonMap(
            "777",
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    final Map<String, ExtendedRecord> expected =
        Collections.singletonMap(
            "777",
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    // When
    Map<String, ExtendedRecord> result =
        ExtensionFilterTransform.create(Collections.emptySet()).transform(input);

    // Should
    assertMap(expected, result);
  }

  @Test
  public void allowExtensionJavaTest() {

    // State
    final Map<String, ExtendedRecord> input =
        Collections.singletonMap(
            "777",
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    final Map<String, ExtendedRecord> expected =
        Collections.singletonMap(
            "777",
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    final Set<String> allowSet = Collections.singleton("ext");

    // When
    Map<String, ExtendedRecord> result = ExtensionFilterTransform.create(allowSet).transform(input);

    // Should
    assertMap(expected, result);
  }

  @Test
  public void excludeExtensionJavaTest() {

    // State
    final Map<String, ExtendedRecord> input =
        Collections.singletonMap(
            "777",
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(
                    Collections.singletonMap(
                        "ext", Collections.singletonList(Collections.singletonMap("test", "test"))))
                .build());

    final Map<String, ExtendedRecord> expected =
        Collections.singletonMap(
            "777",
            ExtendedRecord.newBuilder()
                .setId("777")
                .setCoreTerms(Collections.singletonMap("test", "test"))
                .setExtensions(Collections.emptyMap())
                .build());

    final Set<String> allowSet = Collections.singleton("nop");

    // When
    Map<String, ExtendedRecord> result = ExtensionFilterTransform.create(allowSet).transform(input);

    // Should
    assertMap(expected, result);
  }

  private void assertMap(Map<String, ExtendedRecord> expected, Map<String, ExtendedRecord> result) {
    Assert.assertNotNull(result);
    expected.forEach(
        (k, v) -> {
          ExtendedRecord record = result.get(k);
          Assert.assertNotNull(record);
          Assert.assertEquals(v.getId(), record.getId());
          v.getExtensions()
              .forEach(
                  (ek, ev) -> {
                    List<Map<String, String>> maps = record.getExtensions().get(ek);
                    Assert.assertNotNull(maps);
                    Assert.assertTrue(maps.size() > 0);
                  });
        });
  }
}
