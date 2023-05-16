package org.gbif.pipelines.core.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class IdentificationUtilsTest {

  @Test
  public void getIdentificationFieldTermsSourceOnlyOneExtensionTest() {
    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> identification1 = new HashMap<>(1);
    identification1.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    ext.put(Extension.IDENTIFICATION.getRowType(), Collections.singletonList(identification1));
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setExtensions(ext).build();

    // When
    Map<String, String> source = IdentificationUtils.getIdentificationFieldTermsSource(er);

    // Then
    Assert.assertEquals(1, source.size());
  }

  @Test
  public void getIdentificationFieldTermsSourceMultipleExtensionsWithoutDateTest() {
    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> identification1 = new HashMap<>(1);
    identification1.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    Map<String, String> identification2 = new HashMap<>(1);
    identification2.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    ext.put(Extension.IDENTIFICATION.getRowType(), Arrays.asList(identification1, identification2));
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setExtensions(ext).build();

    // When
    Map<String, String> source = IdentificationUtils.getIdentificationFieldTermsSource(er);

    // Then
    Assert.assertTrue(source.isEmpty());
  }

  @Test
  public void getIdentificationFieldTermsSourceMultipleExtensionsWithDateTest() {
    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> identification1 = new HashMap<>(1);
    identification1.put(DwcTerm.kingdom.qualifiedName(), "k");
    identification1.put(DwcTerm.dateIdentified.qualifiedName(), "2022-09-01T10:00:01");
    Map<String, String> identification2 = new HashMap<>(1);
    identification2.put(DwcTerm.class_.qualifiedName(), "c");
    identification2.put(DwcTerm.dateIdentified.qualifiedName(), "2022-09-01T10:00:00");
    Map<String, String> identification3 = new HashMap<>(1);
    identification3.put(DwcTerm.genus.qualifiedName(), "g");
    identification3.put(DwcTerm.dateIdentified.qualifiedName(), "2022-09-01");
    Map<String, String> identification4 = new HashMap<>(1);
    identification4.put(DwcTerm.family.qualifiedName(), "f");
    identification4.put(DwcTerm.dateIdentified.qualifiedName(), "2022");
    Map<String, String> identification5 = new HashMap<>(1);
    identification5.put(DwcTerm.subfamily.qualifiedName(), "sf");
    identification5.put(DwcTerm.dateIdentified.qualifiedName(), "2022-09-01/02");
    Map<String, String> identification6 = new HashMap<>(1);
    identification6.put(DwcTerm.subgenus.qualifiedName(), "sg");
    identification6.put(DwcTerm.dateIdentified.qualifiedName(), "2022-09");
    Map<String, String> identification7 = new HashMap<>(1);
    identification7.put(DwcTerm.order.qualifiedName(), "0");
    identification7.put(DwcTerm.dateIdentified.qualifiedName(), "2022-09-01T10:00");
    ext.put(
        Extension.IDENTIFICATION.getRowType(),
        Arrays.asList(
            identification1,
            identification2,
            identification3,
            identification4,
            identification5,
            identification6,
            identification7));
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setExtensions(ext).build();

    // When
    Map<String, String> source = IdentificationUtils.getIdentificationFieldTermsSource(er);

    // Then
    Assert.assertEquals(2, source.size());
    Assert.assertTrue(source.containsKey(DwcTerm.kingdom.qualifiedName()));
    Assert.assertFalse(source.containsKey(DwcTerm.class_.qualifiedName()));
    Assert.assertFalse(source.containsKey(DwcTerm.genus.qualifiedName()));
    Assert.assertFalse(source.containsKey(DwcTerm.family.qualifiedName()));
    Assert.assertFalse(source.containsKey(DwcTerm.subfamily.qualifiedName()));
    Assert.assertFalse(source.containsKey(DwcTerm.subgenus.qualifiedName()));
    Assert.assertFalse(source.containsKey(DwcTerm.order.qualifiedName()));
  }

  @Test
  public void getIdentificationFieldTermsSourceMultipleExtensionsWithOnlyOneDateTest() {
    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> identification1 = new HashMap<>(1);
    identification1.put(DwcTerm.kingdom.qualifiedName(), "k");
    identification1.put(DwcTerm.dateIdentified.qualifiedName(), "2022-09-01T10:00:01");
    Map<String, String> identification2 = new HashMap<>(1);
    identification2.put(DwcTerm.class_.qualifiedName(), "c");
    ext.put(Extension.IDENTIFICATION.getRowType(), Arrays.asList(identification1, identification2));
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setExtensions(ext).build();

    // When
    Map<String, String> source = IdentificationUtils.getIdentificationFieldTermsSource(er);

    // Then
    Assert.assertTrue(source.isEmpty());
  }

  @Test
  public void getIdentificationFieldTermsSourceWithCoreTest() {
    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> coreMap = new HashMap<>(1);
    coreMap.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    Map<String, String> identification = new HashMap<>(1);
    identification.put(DwcTerm.class_.qualifiedName(), "Aves");
    ext.put(Extension.IDENTIFICATION.getRowType(), Collections.singletonList(identification));
    ExtendedRecord er =
        ExtendedRecord.newBuilder().setId("1").setCoreTerms(coreMap).setExtensions(ext).build();

    // When
    Map<String, String> source = IdentificationUtils.getIdentificationFieldTermsSource(er);

    // Then
    Assert.assertEquals(1, source.size());
    Assert.assertTrue(source.containsKey(DwcTerm.kingdom.qualifiedName()));
    Assert.assertFalse(source.containsKey(DwcTerm.class_.qualifiedName()));
  }
}
