package org.gbif.pipelines.core.interpreters.metadata;

import java.util.Collections;
import java.util.UUID;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MachineTag;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.TaggedValueRecord;
import org.junit.Assert;
import org.junit.Test;

/** Test class for {@link org.gbif.pipelines.core.interpreters.metadata.TaggedValuesInterpreter} */
public class TaggedValueInterpreterTest {

  /**
   * Creates a generic test case using a tagName and comparing the transformation formTerm to
   * toTerm.
   */
  private void genericTaggedValueTest(String tagName, Term fromTerm, Term toTerm) {
    String key = UUID.randomUUID().toString();
    String testValue = "TEST_VAL";
    MachineTag machineTag =
        MachineTag.newBuilder()
            .setNamespace(TaggedValuesInterpreter.PROCESSING_NAMESPACE)
            .setName(tagName)
            .setValue(key + ":" + testValue)
            .build();
    MetadataRecord mr =
        MetadataRecord.newBuilder()
            .setId("1")
            .setMachineTags(Collections.singletonList(machineTag))
            .build();
    TaggedValueRecord tvr = TaggedValueRecord.newBuilder().setId("1").build();
    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId("1")
            .setCoreTerms(Collections.singletonMap(fromTerm.qualifiedName(), testValue))
            .build();
    TaggedValuesInterpreter.interpret(mr).accept(er, tvr);

    Assert.assertEquals(key, tvr.getTaggedValues().get(toTerm.qualifiedName()));
  }

  /**
   * Test all the combinations of MachineTag to derive GrSciColl collections and institution codes.
   */
  @Test
  public void testCollectionTaggedValues() {

    genericTaggedValueTest(
        TaggedValuesInterpreter.COLLECTION_TAG_NAME,
        DwcTerm.collectionCode,
        GbifInternalTerm.collectionKey);

    genericTaggedValueTest(
        TaggedValuesInterpreter.INSTITUTION_TAG_NAME,
        DwcTerm.institutionCode,
        GbifInternalTerm.institutionKey);

    genericTaggedValueTest(
        TaggedValuesInterpreter.COLLECTION_TO_INSTITUTION_TAG_NAME,
        DwcTerm.collectionCode,
        GbifInternalTerm.institutionKey);

    genericTaggedValueTest(
        TaggedValuesInterpreter.INSTITUTION_TO_COLLECTION_TAG_NAME,
        DwcTerm.institutionCode,
        GbifInternalTerm.collectionKey);
  }
}
