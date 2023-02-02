package org.gbif.pipelines.core.converters;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.pojo.ErIdrMdrContainer;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.extension.dwc.MeasurementOrFactTable;
import org.junit.Assert;
import org.junit.Test;

public class MeasurementOrFactTableConverterTest {

  @Test
  public void measurementOrFactTableTest() {

    // State
    Map<String, String> ext1 = new HashMap<>();
    ext1.put(DwcTerm.measurementID.qualifiedName(), "1");
    ext1.put(DwcTerm.measurementType.qualifiedName(), "Type1");
    ext1.put(DwcTerm.measurementValue.qualifiedName(), "1.5");
    ext1.put(DwcTerm.measurementAccuracy.qualifiedName(), "Accurancy1");
    ext1.put(DwcTerm.measurementUnit.qualifiedName(), "Unit1");
    ext1.put(DwcTerm.measurementDeterminedBy.qualifiedName(), "By1");
    ext1.put(DwcTerm.measurementMethod.qualifiedName(), "Method1");
    ext1.put(DwcTerm.measurementRemarks.qualifiedName(), "Remarks1");
    ext1.put(DwcTerm.measurementDeterminedDate.qualifiedName(), "2011");

    Map<String, String> ext2 = new HashMap<>();
    ext2.put(DwcTerm.measurementID.qualifiedName(), "2");
    ext2.put(DwcTerm.measurementType.qualifiedName(), "Type2");
    ext2.put(DwcTerm.measurementValue.qualifiedName(), "1.5");
    ext2.put(DwcTerm.measurementAccuracy.qualifiedName(), "Accurancy2");
    ext2.put(DwcTerm.measurementUnit.qualifiedName(), "Unit2");
    ext2.put(DwcTerm.measurementDeterminedBy.qualifiedName(), "By2");
    ext2.put(DwcTerm.measurementMethod.qualifiedName(), "Method2");
    ext2.put(DwcTerm.measurementRemarks.qualifiedName(), "Remarks2");
    ext2.put(DwcTerm.measurementDeterminedDate.qualifiedName(), "2012");

    Map<String, List<Map<String, String>>> ext = new HashMap<>();
    ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Arrays.asList(ext1, ext2));

    ExtendedRecord extendedRecord =
        ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    IdentifierRecord identifierRecord =
        IdentifierRecord.newBuilder().setId("777").setInternalId("777").build();

    MetadataRecord metadataRecord =
        MetadataRecord.newBuilder().setId("777").setDatasetKey("dataset_key").build();

    // When
    List<MeasurementOrFactTable> result =
        MeasurementOrFactTableConverter.convert(
            ErIdrMdrContainer.create(extendedRecord, identifierRecord, metadataRecord));

    // Should
    Assert.assertEquals(2, result.size());

    result.forEach(
        table -> {
          String id = table.getVMeasurementid();

          Assert.assertEquals("777", table.getGbifid());
          Assert.assertEquals("dataset_key", table.getDatasetkey());
          // Verbatim
          Assert.assertNotNull(table.getVMeasurementid());
          Assert.assertEquals("Type" + id, table.getVMeasurementtype());
          Assert.assertEquals("1.5", table.getVMeasurementvalue());
          Assert.assertEquals("Accurancy" + id, table.getVMeasurementaccuracy());
          Assert.assertEquals("Unit" + id, table.getVMeasurementunit());
          Assert.assertEquals("By" + id, table.getVMeasurementdeterminedby());
          Assert.assertEquals("Method" + id, table.getVMeasurementmethod());
          Assert.assertEquals("Remarks" + id, table.getVMeasurementremarks());
          Assert.assertEquals("201" + id, table.getVMeasurementdetermineddate());
          // Interpreted
          Assert.assertNotNull(table.getMeasurementid());
          Assert.assertEquals("Type" + id, table.getMeasurementtype());
          Assert.assertEquals("1.5", table.getMeasurementvalue());
          Assert.assertEquals("Accurancy" + id, table.getMeasurementaccuracy());
          Assert.assertEquals("Unit" + id, table.getMeasurementunit());
          Assert.assertEquals("By" + id, table.getMeasurementdeterminedby());
          Assert.assertEquals("Method" + id, table.getMeasurementmethod());
          Assert.assertEquals("Remarks" + id, table.getMeasurementremarks());
          Assert.assertEquals("201" + id, table.getMeasurementdetermineddate());
        });
  }

  @Test
  public void noExtensionTest() {

    // State
    ExtendedRecord extendedRecord = ExtendedRecord.newBuilder().setId("id").build();

    IdentifierRecord identifierRecord =
        IdentifierRecord.newBuilder().setId("777").setInternalId("777").build();

    MetadataRecord metadataRecord =
        MetadataRecord.newBuilder().setId("777").setDatasetKey("dataset_key").build();

    // When
    List<MeasurementOrFactTable> result =
        MeasurementOrFactTableConverter.convert(
            ErIdrMdrContainer.create(extendedRecord, identifierRecord, metadataRecord));

    // Should
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void metadataRecordNullTest() {
    // State
    MetadataRecord metadataRecord =
        MetadataRecord.newBuilder().setId("777").setDatasetKey("dataset_key").build();

    // When
    List<MeasurementOrFactTable> result =
        MeasurementOrFactTableConverter.convert(
            ErIdrMdrContainer.create(null, null, metadataRecord));

    // Should
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void identifierRecordNullTest() {
    // State
    IdentifierRecord identifierRecord =
        IdentifierRecord.newBuilder().setId("777").setInternalId("777").build();

    // When
    List<MeasurementOrFactTable> result =
        MeasurementOrFactTableConverter.convert(
            ErIdrMdrContainer.create(null, identifierRecord, null));

    // Should
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void extendedRecordNullTest() {

    // State
    Map<String, String> ext1 = new HashMap<>();
    ext1.put(DwcTerm.measurementID.qualifiedName(), "Id1");
    ext1.put(DwcTerm.measurementType.qualifiedName(), "Type1");
    ext1.put(DwcTerm.measurementValue.qualifiedName(), "1.5");
    ext1.put(DwcTerm.measurementAccuracy.qualifiedName(), "Accurancy1");
    ext1.put(DwcTerm.measurementUnit.qualifiedName(), "Unit1");
    ext1.put(DwcTerm.measurementDeterminedBy.qualifiedName(), "By1");
    ext1.put(DwcTerm.measurementMethod.qualifiedName(), "Method1");
    ext1.put(DwcTerm.measurementRemarks.qualifiedName(), "Remarks1");
    ext1.put(DwcTerm.measurementDeterminedDate.qualifiedName(), "2010/2011");

    Map<String, List<Map<String, String>>> ext = new HashMap<>();
    ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Collections.singletonList(ext1));

    ExtendedRecord extendedRecord =
        ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    // When
    List<MeasurementOrFactTable> result =
        MeasurementOrFactTableConverter.convert(
            ErIdrMdrContainer.create(extendedRecord, null, null));

    // Should
    Assert.assertTrue(result.isEmpty());
  }
}
