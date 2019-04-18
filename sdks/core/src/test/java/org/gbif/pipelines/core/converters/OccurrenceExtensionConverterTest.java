package org.gbif.pipelines.core.converters;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.junit.Assert;
import org.junit.Test;

public class OccurrenceExtensionConverterTest {

  @Test
  public void noOccurrenceAsExtensionTest() {
    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").build();

    // When
    List<ExtendedRecord> result = OccurrenceExtensionConverter.convert(er);

    // Should
    Assert.assertNotNull(result);
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void occurrenceAsExtensionTest() {
    // State
    String id = "1";
    String somethingCore = "somethingCore";
    String somethingExt = "somethingExt";

    Map<String, String> ext1 = new HashMap<>(2);
    ext1.put(DwcTerm.occurrenceID.qualifiedName(), id);
    ext1.put(somethingExt, somethingExt);

    Map<String, String> ext2 = new HashMap<>(2);
    ext2.put(DwcTerm.occurrenceID.qualifiedName(), id);
    ext2.put(somethingExt, somethingExt);

    Map<String, String> ext3 = new HashMap<>(2);
    ext3.put(DwcTerm.occurrenceID.qualifiedName(), id);
    ext3.put(somethingExt, somethingExt);

    ExtendedRecord er = ExtendedRecord.newBuilder()
        .setId(id)
        .setCoreTerms(Collections.singletonMap(somethingCore, somethingCore))
        .setExtensions(Collections.singletonMap(DwcTerm.Occurrence.qualifiedName(), Arrays.asList(ext1, ext2, ext3)))
        .build();

    // When
    List<ExtendedRecord> result = OccurrenceExtensionConverter.convert(er);

    // Should
    Assert.assertNotNull(result);
    Assert.assertEquals(3, result.size());
    result.forEach(x -> {
      Assert.assertEquals(id, x.getId());
      Assert.assertEquals(somethingCore, x.getCoreTerms().get(somethingCore));
      Assert.assertEquals(somethingExt, x.getCoreTerms().get(somethingExt));
    });
  }

}
