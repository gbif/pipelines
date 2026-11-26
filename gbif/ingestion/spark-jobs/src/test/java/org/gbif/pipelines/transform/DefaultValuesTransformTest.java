package org.gbif.pipelines.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.gbif.api.vocabulary.TagNamespace;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MachineTag;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.junit.Test;

public class DefaultValuesTransformTest {

  private static MachineTag tag(String namespace, String name, String value) {
    return MachineTag.newBuilder().setNamespace(namespace).setName(name).setValue(value).build();
  }

  private static ExtendedRecord record(Map<String, String> coreTerms) {
    return ExtendedRecord.newBuilder().setId("1").setCoreTerms(coreTerms).build();
  }

  @Test
  public void onlyDefaultTermNamespaceTagsAreApplied() {
    String defaultNs = TagNamespace.GBIF_DEFAULT_TERM.getNamespace();
    MetadataRecord mdr =
        MetadataRecord.newBuilder()
            .setId("1")
            .setMachineTags(
                Arrays.asList(
                    tag(defaultNs, "country", "DK"),
                    tag("crawler.gbif.org", "crawl_attempt", "42"),
                    tag("orphans.gbif.org", "status", "orphan"),
                    tag("some.other.ns", "basisOfRecord", "FOSSIL_SPECIMEN")))
            .build();

    ExtendedRecord er = record(new HashMap<>());
    ExtendedRecord result = DefaultValuesTransform.create(null, mdr).convert(er);

    Map<String, String> terms = result.getCoreTerms();
    assertEquals("DK", terms.get(DwcTerm.country.qualifiedName()));
    assertNull(terms.get(DwcTerm.basisOfRecord.qualifiedName()));
    assertEquals(1, terms.size());
  }

  @Test
  public void existingValuesAreNotOverwritten() {
    MetadataRecord mdr =
        MetadataRecord.newBuilder()
            .setId("1")
            .setMachineTags(
                Arrays.asList(tag(TagNamespace.GBIF_DEFAULT_TERM.getNamespace(), "country", "DK")))
            .build();

    Map<String, String> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.country.qualifiedName(), "SE");
    ExtendedRecord result = DefaultValuesTransform.create(null, mdr).convert(record(coreTerms));

    assertEquals("SE", result.getCoreTerms().get(DwcTerm.country.qualifiedName()));
  }
}
