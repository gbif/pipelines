package org.gbif.converters.parser.xml.model;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import org.gbif.converters.parser.xml.constants.PrioritizedPropertyNameEnum;
import org.gbif.converters.parser.xml.parsing.xml.PrioritizedProperty;
import org.junit.Assert;
import org.junit.Test;

public class PropertyPrioritizerTest {

  @Test
  public void findHighestPriorityTest() {

    // State
    String expected = "Aa";
    Set<PrioritizedProperty> set =
        new TreeSet<>(Comparator.comparing(PrioritizedProperty::getProperty));
    set.add(new PrioritizedProperty(PrioritizedPropertyNameEnum.COLLECTOR_NAME, 1, "Aa"));
    set.add(new PrioritizedProperty(PrioritizedPropertyNameEnum.COLLECTOR_NAME, 1, "Bb"));
    set.add(new PrioritizedProperty(PrioritizedPropertyNameEnum.COLLECTOR_NAME, 1, "Cc"));

    // When
    String result = PropertyPrioritizer.findHighestPriority(set);

    // Should
    Assert.assertEquals(expected, result);
  }

  @Test
  public void reverseFindHighestPriorityTest() {

    // State
    String expected = "Aa";
    Set<PrioritizedProperty> set =
        new TreeSet<>(Comparator.comparing(PrioritizedProperty::getProperty).reversed());
    set.add(new PrioritizedProperty(PrioritizedPropertyNameEnum.COLLECTOR_NAME, 1, "Cc"));
    set.add(new PrioritizedProperty(PrioritizedPropertyNameEnum.COLLECTOR_NAME, 1, "Bb"));
    set.add(new PrioritizedProperty(PrioritizedPropertyNameEnum.COLLECTOR_NAME, 1, "Aa"));

    // When
    String result = PropertyPrioritizer.findHighestPriority(set);

    // Should
    Assert.assertEquals(expected, result);
  }
}
