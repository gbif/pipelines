package org.gbif.pipelines.transforms.java;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
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
    final Map<String, GbifIdRecord> expected = createIdMap("1_1", "2_2", "3_3", "4_4");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .skipTransform(true)
            .build()
            .run();

    Map<String, GbifIdRecord> idMap = gbifIdTransform.getIdMap();
    Map<String, GbifIdRecord> idInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(expected.size(), idMap.size());
    Assert.assertEquals(0, idInvalidMap.size());
    assertMap(expected, idMap);
  }

  @Test
  public void withoutDuplicatesTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1_1", "2_2", "3_3", "4_4", "5_5", "6_6");
    final Map<String, GbifIdRecord> expected =
        createGbifIdMap("1_1", "2_2", "3_3", "4_4", "5_5", "6_6");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .build()
            .run();

    Map<String, GbifIdRecord> idMap = gbifIdTransform.getIdMap();
    Map<String, GbifIdRecord> idInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(expected.size(), idMap.size());
    Assert.assertEquals(0, idInvalidMap.size());
    assertMap(expected, idMap);
  }

  @Test
  public void allDuplicatesTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1_1", "2_1", "3_1", "4_1", "5_1", "6_1");
    final Map<String, GbifIdRecord> expectedNormal = createGbifIdMap("4_1");
    final Map<String, GbifIdRecord> expectedInvalid =
        createIdMap("1_1", "2_1", "3_1", "5_1", "6_1");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .build()
            .run();

    Map<String, GbifIdRecord> idMap = gbifIdTransform.getIdMap();
    Map<String, GbifIdRecord> idInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(expectedNormal.size(), idMap.size());
    Assert.assertEquals(expectedInvalid.size(), idInvalidMap.size());
    assertMap(expectedNormal, idMap);
    assertMap(expectedInvalid, idInvalidMap);
  }

  @Test
  public void noGbifIdTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1", "2", "3", "4", "5", "6");
    final Map<String, GbifIdRecord> expectedInvalid = createIdMap("1", "2", "3", "4", "5", "6");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .build()
            .run();

    Map<String, GbifIdRecord> idMap = gbifIdTransform.getIdMap();
    Map<String, GbifIdRecord> idInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(0, idMap.size());
    Assert.assertEquals(expectedInvalid.size(), idInvalidMap.size());
    assertMap(expectedInvalid, idInvalidMap);
  }

  @Test
  public void oneValueTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1_1");
    final Map<String, GbifIdRecord> expectedNormal = createGbifIdMap("1_1");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .build()
            .run();

    Map<String, GbifIdRecord> idMap = gbifIdTransform.getIdMap();
    Map<String, GbifIdRecord> idInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(expectedNormal.size(), idMap.size());
    Assert.assertEquals(0, idInvalidMap.size());
    assertMap(expectedNormal, idMap);
  }

  @Test
  public void oneWithoutGbifIdValueTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1");
    final Map<String, GbifIdRecord> expectedInvalid = createIdMap("1");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .build()
            .run();

    Map<String, GbifIdRecord> idMap = gbifIdTransform.getIdMap();
    Map<String, GbifIdRecord> idInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(0, idMap.size());
    Assert.assertEquals(expectedInvalid.size(), idInvalidMap.size());
    assertMap(expectedInvalid, idInvalidMap);
  }

  @Test
  public void mixedValuesSyncTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1", "2_2", "3_3", "4_1", "5", "6_6");
    final Map<String, GbifIdRecord> expectedNormal = createGbifIdMap("2_2", "3_3", "4_1", "6_6");
    final Map<String, GbifIdRecord> expectedInvalid = createIdMap("1", "5");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .useSyncMode(true)
            .build()
            .run();

    Map<String, GbifIdRecord> idMap = gbifIdTransform.getIdMap();
    Map<String, GbifIdRecord> idInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(expectedNormal.size(), idMap.size());
    Assert.assertEquals(expectedInvalid.size(), idInvalidMap.size());
    assertMap(expectedNormal, idMap);
    assertMap(expectedInvalid, idInvalidMap);
  }

  @Test
  public void mixedValuesAsyncTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1", "2_2", "3_3", "4_1", "5", "6_6");
    final Map<String, GbifIdRecord> expectedNormal = createGbifIdMap("2_2", "3_3", "4_1", "6_6");
    final Map<String, GbifIdRecord> expectedInvalid = createIdMap("1", "5");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .idTransformFn(basicTransform::processElement)
            .useSyncMode(false)
            .build()
            .run();

    Map<String, GbifIdRecord> idMap = gbifIdTransform.getIdMap();
    Map<String, GbifIdRecord> idInvalidMap = gbifIdTransform.getIdInvalidMap();

    // Should
    Assert.assertEquals(expectedNormal.size(), idMap.size());
    Assert.assertEquals(expectedInvalid.size(), idInvalidMap.size());
    assertMap(expectedNormal, idMap);
    assertMap(expectedInvalid, idInvalidMap);
  }

  private static <K> void assertMap(Map<K, GbifIdRecord> expected, Map<K, GbifIdRecord> result) {
    expected.forEach(
        (k, v) -> {
          GbifIdRecord record = result.get(k);
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

  private Map<String, GbifIdRecord> createGbifIdMap(String... idName) {
    return Arrays.stream(idName)
        .map(
            x -> {
              String[] array = x.split("_");
              return GbifIdRecord.newBuilder()
                  .setId(array[0])
                  .setGbifId(array.length > 1 ? Long.valueOf(array[1]) : null)
                  .build();
            })
        .collect(Collectors.toMap(id -> id.getGbifId().toString(), Function.identity()));
  }

  private Map<String, GbifIdRecord> createIdMap(String... idName) {
    return Arrays.stream(idName)
        .map(
            x -> {
              String[] array = x.split("_");
              return GbifIdRecord.newBuilder()
                  .setId(array[0])
                  .setGbifId(array.length > 1 ? Long.valueOf(array[1]) : null)
                  .build();
            })
        .collect(Collectors.toMap(GbifIdRecord::getId, Function.identity()));
  }
}
