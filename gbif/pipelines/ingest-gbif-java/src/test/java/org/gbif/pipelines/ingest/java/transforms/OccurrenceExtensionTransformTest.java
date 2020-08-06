package org.gbif.pipelines.ingest.java.transforms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class OccurrenceExtensionTransformTest {

  @Test
  public void extraOccInExtTest() {

    // State
    List<Map<String, String>> ext = new ArrayList<>();
    ext.add(Collections.singletonMap(DwcTerm.occurrenceID.qualifiedName(), "value1"));
    ext.add(Collections.singletonMap(DwcTerm.occurrenceID.qualifiedName(), "value2"));
    ext.add(Collections.singletonMap(DwcTerm.occurrenceID.qualifiedName(), "value3"));
    ExtendedRecord record =
        ExtendedRecord.newBuilder()
            .setId("777")
            .setCoreTerms(Collections.singletonMap("key1", "value1"))
            .setExtensions(Collections.singletonMap(DwcTerm.Occurrence.qualifiedName(), ext))
            .build();
    Map<String, ExtendedRecord> value = Collections.singletonMap(record.getId(), record);

    AtomicInteger counter = new AtomicInteger(0);

    // When
    Map<String, ExtendedRecord> result =
        OccurrenceExtensionTransform.create()
            .counterFn(s -> counter.incrementAndGet())
            .transform(value);

    // Should
    Assert.assertEquals(3, counter.get());
    Assert.assertEquals(3, result.size());
  }

  @Test
  public void extraOccIdIsEmptyTest() {

    // State
    List<Map<String, String>> ext = new ArrayList<>();
    ext.add(Collections.singletonMap(DwcTerm.occurrenceID.qualifiedName(), "value1"));
    ext.add(Collections.singletonMap(DwcTerm.occurrenceID.qualifiedName(), ""));
    ExtendedRecord record =
        ExtendedRecord.newBuilder()
            .setId("")
            .setCoreTerms(Collections.singletonMap("key1", "value1"))
            .setExtensions(Collections.singletonMap(DwcTerm.Occurrence.qualifiedName(), ext))
            .build();
    Map<String, ExtendedRecord> value = Collections.singletonMap(record.getId(), record);

    AtomicInteger counter = new AtomicInteger(0);

    // When
    Map<String, ExtendedRecord> result =
        OccurrenceExtensionTransform.create()
            .counterFn(s -> counter.incrementAndGet())
            .transform(value);

    // Should
    Assert.assertEquals(1, counter.get());
    Assert.assertEquals(1, result.size());
  }
}
