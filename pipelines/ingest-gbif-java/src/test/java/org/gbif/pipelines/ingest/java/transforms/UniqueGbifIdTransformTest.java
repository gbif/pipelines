package org.gbif.pipelines.ingest.java.transforms;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;

import org.junit.Assert;
import org.junit.Test;

public class UniqueGbifIdTransformTest {

  private final String key = "key";
  private final BiConsumer<ExtendedRecord, BasicRecord> gbifIdFn =
      (er, br) -> Optional.ofNullable(er.getCoreTerms().get(key)).ifPresent(x -> br.setGbifId(Long.valueOf(x)));
  private final BasicTransform basicTransform = BasicTransform.create(gbifIdFn);

  @Test
  public void withoutDuplicatesTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1_1", "2_2", "3_3", "4_4", "5_5", "6_6");
    final Map<Long, BasicRecord> expected = createBrLongMap("1_1", "2_2", "3_3", "4_4", "5_5", "6_6");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .basicTransform(basicTransform)
            .build()
            .run();

    Map<Long, BasicRecord> brMap = gbifIdTransform.getBrMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getBrInvalidMap();

    // Should
    Assert.assertEquals(expected.size(), brMap.size());
    Assert.assertEquals(0, brInvalidMap.size());
    assertMap(expected, brMap);
  }

  @Test
  public void allDuplicatesTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1_1", "2_1", "3_1", "4_1", "5_1", "6_1");
    final Map<Long, BasicRecord> expectedNormal = createBrLongMap("4_1");
    final Map<String, BasicRecord> expectedInvalid = createBrStringMap("1_1", "2_1", "3_1", "5_1", "6_1");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .basicTransform(basicTransform)
            .build()
            .run();

    Map<Long, BasicRecord> brMap = gbifIdTransform.getBrMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getBrInvalidMap();


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
    final Map<String, BasicRecord> expectedInvalid = createBrStringMap("1", "2", "3", "4", "5", "6");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .basicTransform(basicTransform)
            .build()
            .run();

    Map<Long, BasicRecord> brMap = gbifIdTransform.getBrMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getBrInvalidMap();

    // Should
    Assert.assertEquals(0, brMap.size());
    Assert.assertEquals(expectedInvalid.size(), brInvalidMap.size());
    assertMap(expectedInvalid, brInvalidMap);
  }

  @Test
  public void oneValueTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1_1");
    final Map<Long, BasicRecord> expectedNormal = createBrLongMap("1_1");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .basicTransform(basicTransform)
            .build()
            .run();

    Map<Long, BasicRecord> brMap = gbifIdTransform.getBrMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getBrInvalidMap();

    // Should
    Assert.assertEquals(expectedNormal.size(), brMap.size());
    Assert.assertEquals(0, brInvalidMap.size());
    assertMap(expectedNormal, brMap);
  }

  @Test
  public void oneWithoutGbifIdValueTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1");
    final Map<String, BasicRecord> expectedInvalid = createBrStringMap("1");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .basicTransform(basicTransform)
            .build()
            .run();

    Map<Long, BasicRecord> brMap = gbifIdTransform.getBrMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getBrInvalidMap();

    // Should
    Assert.assertEquals(0, brMap.size());
    Assert.assertEquals(expectedInvalid.size(), brInvalidMap.size());
    assertMap(expectedInvalid, brInvalidMap);
  }

  @Test
  public void mixedValuesTest() {
    // State
    final Map<String, ExtendedRecord> input = createErMap("1", "2_2", "3_3", "4_1", "5", "6_6");
    final Map<Long, BasicRecord> expectedNormal = createBrLongMap("2_2", "3_3", "4_1", "6_6");
    final Map<String, BasicRecord> expectedInvalid = createBrStringMap("1", "5");

    // When
    UniqueGbifIdTransform gbifIdTransform =
        UniqueGbifIdTransform.builder()
            .erMap(input)
            .basicTransform(basicTransform)
            .build()
            .run();

    Map<Long, BasicRecord> brMap = gbifIdTransform.getBrMap();
    Map<String, BasicRecord> brInvalidMap = gbifIdTransform.getBrInvalidMap();

    // Should
    Assert.assertEquals(expectedNormal.size(), brMap.size());
    Assert.assertEquals(expectedInvalid.size(), brInvalidMap.size());
    assertMap(expectedNormal, brMap);
    assertMap(expectedInvalid, brInvalidMap);
  }

  private static <K> void assertMap(Map<K, BasicRecord> expected, Map<K, BasicRecord> result) {
    expected.forEach((k, v) -> {
      BasicRecord record = result.get(k);
      Assert.assertNotNull(record);
      Assert.assertEquals(v.getId(), record.getId());
      Assert.assertEquals(v.getGbifId(), record.getGbifId());
    });
  }

  private Map<String, ExtendedRecord> createErMap(String... idName) {
    return Arrays.stream(idName)
        .map(x -> {
          String[] array = x.split("_");
          return ExtendedRecord.newBuilder()
              .setId(array[0])
              .setCoreTerms(Collections.singletonMap("key", array.length > 1 ? array[1] : null))
              .build();
        })
        .collect(Collectors.toMap(ExtendedRecord::getId, Function.identity()));
  }

  private Map<Long, BasicRecord> createBrLongMap(String... idName) {
    return Arrays.stream(idName)
        .map(x -> {
          String[] array = x.split("_");
          return BasicRecord.newBuilder()
              .setId(array[0])
              .setGbifId(array.length > 1 ? Long.valueOf(array[1]) : null)
              .build();
        })
        .collect(Collectors.toMap(BasicRecord::getGbifId, Function.identity()));
  }

  private Map<String, BasicRecord> createBrStringMap(String... idName) {
    return Arrays.stream(idName)
        .map(x -> {
          String[] array = x.split("_");
          return BasicRecord.newBuilder()
              .setId(array[0])
              .setGbifId(array.length > 1 ? Long.valueOf(array[1]) : null)
              .build();
        })
        .collect(Collectors.toMap(BasicRecord::getId, Function.identity()));

  }
}
