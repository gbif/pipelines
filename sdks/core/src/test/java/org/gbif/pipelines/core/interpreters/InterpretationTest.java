package org.gbif.pipelines.core.interpreters;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.Assert;
import org.junit.Test;

public class InterpretationTest {

  @Test
  public void interpretationTest() {

    // State
    Integer source = 2;

    // When
    Optional<StringBuilder> result =
        Interpretation.from(source)
            .to((Supplier<StringBuilder>) StringBuilder::new)
            .when(i -> i > 0)
            .via((s, t) -> t.append(s + 1))
            .skipWhen(i -> !i.toString().equals("1"))
            .skipWhen(i -> !i.toString().equals("3"))
            .get();

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("3", result.get().toString());
  }

  @Test
  public void interpretationConsumerTest() {

    // State
    List<String> list = new ArrayList<>(1);

    // When
    Interpretation.from(2)
        .to((Supplier<StringBuilder>) StringBuilder::new)
        .when(i -> i > 0)
        .via((s, t) -> t.append(s + 1))
        .consume(x -> list.add(x.toString()));

    // Should
    Assert.assertFalse(list.isEmpty());
    Assert.assertEquals("3", list.get(0));
  }

  @Test
  public void interpretationWhenTest() {

    // State
    Integer source = -1;

    // When
    Optional<StringBuilder> result =
        Interpretation.from(source)
            .to(new StringBuilder())
            .when(i -> i > 0)
            .via((s, t) -> t.append(s + 1))
            .skipWhen(i -> !i.toString().equals("1"))
            .skipWhen(i -> !i.toString().equals("3"))
            .getOfNullable();

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void interpretationSkipWhenTest() {

    // When
    Optional<StringBuilder> result =
        Interpretation.from(() -> 1)
            .to(new StringBuilder())
            .when(i -> i > 0)
            .via((s, t) -> t.append(s + 1))
            .skipWhen(i -> !i.toString().equals("1"))
            .skipWhen(i -> !i.toString().equals("3"))
            .get();

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void interpretationGetOfNullableTest() {

    // When
    Optional<StringBuilder> result =
        Interpretation.from(() -> 1)
            .to(new StringBuilder())
            .when(i -> i > 0)
            .via((t) -> t.append(1))
            .skipWhen(i -> !i.toString().equals("1"))
            .skipWhen(i -> !i.toString().equals("3"))
            .getOfNullable();

    // Should
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals("1", result.get().toString());
  }
}
