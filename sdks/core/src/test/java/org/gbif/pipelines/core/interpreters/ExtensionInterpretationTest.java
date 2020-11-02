package org.gbif.pipelines.core.interpreters;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class ExtensionInterpretationTest {

  @AllArgsConstructor
  @NoArgsConstructor
  @EqualsAndHashCode
  private static class EiTest {

    String val1;
    String val2;
    String val3;
    String val4;
    String val5;
    String val6;
  }

  @Test
  public void interpretationTest() {

    // Expected
    EiTest e = new EiTest("val1val2val3", "val1val2val3val3", "val3", "val4", "val5+", "val6");
    Set<String> issues = new TreeSet<>();
    issues.add("multi");
    issues.add("single");
    Result<EiTest> expected = new Result<>(Collections.singletonList(e), issues);

    Set<String> issuesEr = new TreeSet<>();
    issuesEr.add("checker");
    issuesEr.add("multi");
    issuesEr.add("single");
    Result<EiTest> expectedEr = new Result<>(Collections.singletonList(e), issuesEr);

    // State
    Map<String, String> source = new HashMap<>(3);
    source.put("val1", "val1");
    source.put("val3", "val3");
    source.put(DcTerm.abstract_.qualifiedName(), "val2");
    source.put("val4", "val4");
    source.put("val5", "val5");
    source.put(DcTerm.accessRights.qualifiedName(), "val6");

    ExtendedRecord erSource =
        ExtendedRecord.newBuilder()
            .setId("id")
            .setExtensions(
                Collections.singletonMap(
                    "mmm", Arrays.asList(source, Collections.singletonMap("val1", ""))))
            .build();

    // When
    BiConsumer<EiTest, String> val1Fn = (eiTest, s) -> eiTest.val1 = s;
    BiFunction<EiTest, String, List<String>> val2Fn =
        (eiTest, s) -> {
          eiTest.val2 = s;
          return Collections.emptyList();
        };
    BiFunction<EiTest, String, List<String>> val3Fn =
        (eiTest, s) -> {
          eiTest.val3 = s;
          return Collections.singletonList("single");
        };
    BiFunction<EiTest, String, List<String>> val4Fn =
        (eiTest, s) -> {
          eiTest.val4 = s;
          return Collections.singletonList("multi");
        };
    BiFunction<EiTest, String, String> val5Fn =
        (eiTest, s) -> {
          eiTest.val5 = s;
          return "multi";
        };
    BiFunction<EiTest, String, String> val6Fn =
        (eiTest, s) -> {
          eiTest.val6 = s;
          return "multi";
        };
    Consumer<EiTest> postFn1 = eiTest -> eiTest.val1 = eiTest.val1 + eiTest.val2 + eiTest.val3;
    Function<EiTest, List<String>> postFn2 =
        eiTest -> {
          eiTest.val2 = eiTest.val1 + eiTest.val3;
          return Collections.singletonList("single");
        };
    Function<EiTest, String> postFn3 =
        eiTest -> {
          eiTest.val5 += "+";
          return "";
        };
    Function<EiTest, Optional<String>> checker =
        eit -> {
          if (eit.val1.contains("null")) {
            return Optional.of("checker");
          }
          return Optional.empty();
        };

    TargetHandler<EiTest> handler =
        ExtensionInterpretation.extension("mmm")
            .to(EiTest::new)
            .map("val1", val1Fn)
            .map(DcTerm.abstract_, val2Fn)
            .map("val3", val3Fn)
            .map("val4", val4Fn)
            .mapOne("val5", val5Fn)
            .mapOne(DcTerm.accessRights, val6Fn)
            .postMap(postFn1)
            .postMap(postFn2)
            .postMapOne(postFn3)
            .skipIf(checker);

    Result<EiTest> result1 = handler.convert(source);
    Result<EiTest> result2 = handler.convert(Collections.singletonList(source));
    Result<EiTest> resultEr = handler.convert(erSource);

    // Should
    Assert.assertEquals(expected.getList(), result1.getList());
    Assert.assertEquals(expected.getIssues(), result1.getIssues());

    Assert.assertEquals(expected.getList(), result2.getList());
    Assert.assertEquals(expected.getIssues(), result2.getIssues());

    Assert.assertEquals(expectedEr.getList(), resultEr.getList());
    Assert.assertEquals(expectedEr.getIssues(), resultEr.getIssues());
  }
}
