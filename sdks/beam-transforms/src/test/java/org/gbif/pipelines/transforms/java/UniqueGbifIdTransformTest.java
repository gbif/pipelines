package org.gbif.pipelines.transforms.java;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("all")
public class UniqueGbifIdTransformTest {

  private static final String KEY = "KEY";
  private final BiConsumer<ExtendedRecord, GbifIdRecord> gbifIdFn =
      (er, id) ->
          Optional.ofNullable(er.getCoreTerms().get(KEY))
              .ifPresent(x -> id.setGbifId(Long.valueOf(x)));
  private final GbifIdTransform basicTransform =
      GbifIdTransform.builder().gbifIdFn(gbifIdFn).useExtendedRecordId(true).create();

  @Test
  public void skipFunctionTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1_1", "2_2", "3_3", "4_4");
    final Map<String, BasicRecord> expected = createBrIdMap("1_1", "2_2", "3_3", "4_4");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .skipTransform(true)
            .build()
            .run();

    Map<String, BasicRecord> brMap = gbifIdTransform.getIdMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(expected.size(), brMap.size());
    Assert.assertEquals(0, brInvalidMap.size());
    assertMap(expected, brMap);
  }

  @Test
  public void withoutDuplicatesTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1_1", "2_2", "3_3", "4_4", "5_5", "6_6");
    final Map<String, BasicRecord> expected =
        createBrGbifIdMap("1_1", "2_2", "3_3", "4_4", "5_5", "6_6");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .build()
            .run();

    Map<String, BasicRecord> brMap = gbifIdTransform.getIdMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(expected.size(), brMap.size());
    Assert.assertEquals(0, brInvalidMap.size());
    assertMap(expected, brMap);
  }

  @Test
  public void allDuplicatesTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1_1", "2_1", "3_1", "4_1", "5_1", "6_1");
    final Map<String, BasicRecord> expectedNormal = createBrGbifIdMap("4_1");
    final Map<String, BasicRecord> expectedInvalid =
        createBrIdMap("1_1", "2_1", "3_1", "5_1", "6_1");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .build()
            .run();

    Map<String, BasicRecord> brMap = gbifIdTransform.getIdMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(expectedNormal.size(), brMap.size());
    Assert.assertEquals(expectedInvalid.size(), brInvalidMap.size());
    assertMap(expectedNormal, brMap);
    assertMap(expectedInvalid, brInvalidMap);
  }

  @Test
  public void noGbifIdTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1", "2", "3", "4", "5", "6");
    final Map<String, BasicRecord> expectedInvalid = createBrIdMap("1", "2", "3", "4", "5", "6");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .build()
            .run();

    Map<String, BasicRecord> brMap = gbifIdTransform.getIdMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(0, brMap.size());
    Assert.assertEquals(expectedInvalid.size(), brInvalidMap.size());
    assertMap(expectedInvalid, brInvalidMap);
  }

  @Test
  public void oneValueTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1_1");
    final Map<String, BasicRecord> expectedNormal = createBrGbifIdMap("1_1");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .build()
            .run();

    Map<String, BasicRecord> brMap = gbifIdTransform.getIdMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(expectedNormal.size(), brMap.size());
    Assert.assertEquals(0, brInvalidMap.size());
    assertMap(expectedNormal, brMap);
  }

  @Test
  public void oneWithoutGbifIdValueTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1");
    final Map<String, BasicRecord> expectedInvalid = createBrIdMap("1");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .build()
            .run();

    Map<String, BasicRecord> brMap = gbifIdTransform.getIdMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(0, brMap.size());
    Assert.assertEquals(expectedInvalid.size(), brInvalidMap.size());
    assertMap(expectedInvalid, brInvalidMap);
  }

  @Test
  public void mixedValuesSyncTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1", "2_2", "3_3", "4_1", "5", "6_6");
    final Map<String, BasicRecord> expectedNormal = createBrGbifIdMap("2_2", "3_3", "4_1", "6_6");
    final Map<String, BasicRecord> expectedInvalid = createBrIdMap("1", "5");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .useSyncMode(true)
            .build()
            .run();

    Map<String, BasicRecord> brMap = gbifIdTransform.getIdMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(expectedNormal.size(), brMap.size());
    Assert.assertEquals(expectedInvalid.size(), brInvalidMap.size());
    assertMap(expectedNormal, brMap);
    assertMap(expectedInvalid, brInvalidMap);
  }

  @Test
  public void mixedValuesAsyncTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1", "2_2", "3_3", "4_1", "5", "6_6");
    final Map<String, BasicRecord> expectedNormal = createBrGbifIdMap("2_2", "3_3", "4_1", "6_6");
    final Map<String, BasicRecord> expectedInvalid = createBrIdMap("1", "5");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .useSyncMode(false)
            .build()
            .run();

    Map<String, BasicRecord> brMap = gbifIdTransform.getIdMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(expectedNormal.size(), brMap.size());
    Assert.assertEquals(expectedInvalid.size(), brInvalidMap.size());
    assertMap(expectedNormal, brMap);
    assertMap(expectedInvalid, brInvalidMap);
  }

  private static <K> void assertMap(Map<K, BasicRecord> expected, Map<K, BasicRecord> result) {
    expected.forEach(
        (k, v) -> {
          BasicRecord record = result.get(k);
          Assert.assertNotNull(record);
          Assert.assertEquals(v.getId(), record.getId());
          Assert.assertEquals(v.getGbifId(), record.getGbifId());
        });
  }

  private Map<String, ExtendedRecord> createErMap(String... idName) {
    return Arrays.stream(idName)
        .map(
            x -> {
              String[] array = x.split("_");
              return ExtendedRecord.newBuilder()
                  .setId(array[0])
                  .setCoreTerms(Collections.singletonMap("KEY", array.length > 1 ? array[1] : null))
                  .build();
            })
        .collect(Collectors.toMap(ExtendedRecord::getId, Function.identity()));
  }

  private Map<String, BasicRecord> createBrGbifIdMap(String... idName) {
    return Arrays.stream(idName)
        .map(
            x -> {
              String[] array = x.split("_");
              return BasicRecord.newBuilder()
                  .setId(array[0])
                  .setGbifId(array.length > 1 ? Long.valueOf(array[1]) : null)
                  .build();
            })
        .collect(Collectors.toMap(br -> br.getGbifId().toString(), Function.identity()));
  }

  private Map<String, BasicRecord> createBrIdMap(String... idName) {
    return Arrays.stream(idName)
        .map(
            x -> {
              String[] array = x.split("_");
              return BasicRecord.newBuilder()
                  .setId(array[0])
                  .setGbifId(array.length > 1 ? Long.valueOf(array[1]) : null)
                  .build();
            })
        .collect(Collectors.toMap(BasicRecord::getId, Function.identity()));
  }
}
