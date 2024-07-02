package org.gbif.pipelines.core.converters;

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
    Map<String, String> ext1 =
        Map.of(
            DwcTerm.measurementID.qualifiedName(), "1",
            DwcTerm.measurementType.qualifiedName(), "Type1",
            DwcTerm.measurementValue.qualifiedName(), "1.5",
            DwcTerm.measurementAccuracy.qualifiedName(), "Accurancy1",
            DwcTerm.measurementUnit.qualifiedName(), "Unit1",
            DwcTerm.measurementDeterminedBy.qualifiedName(), "By1",
            DwcTerm.measurementMethod.qualifiedName(), "Method1",
            DwcTerm.measurementRemarks.qualifiedName(), "Remarks1",
            DwcTerm.measurementDeterminedDate.qualifiedName(), "2011");

    Map<String, String> ext2 =
        Map.of(
            DwcTerm.measurementID.qualifiedName(), "2",
            DwcTerm.measurementType.qualifiedName(), "Type2",
            DwcTerm.measurementValue.qualifiedName(), "1.5",
            DwcTerm.measurementAccuracy.qualifiedName(), "Accurancy2",
            DwcTerm.measurementUnit.qualifiedName(), "Unit2",
            DwcTerm.measurementDeterminedBy.qualifiedName(), "By2",
            DwcTerm.measurementMethod.qualifiedName(), "Method2",
            DwcTerm.measurementRemarks.qualifiedName(), "Remarks2",
            DwcTerm.measurementDeterminedDate.qualifiedName(), "2012");

    Map<String, List<Map<String, String>>> ext =
        Map.of(Extension.MEASUREMENT_OR_FACT.getRowType(), List.of(ext1, ext2));

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
    Map<String, String> ext1 =
        Map.of(
            DwcTerm.measurementID.qualifiedName(), "Id1",
            DwcTerm.measurementType.qualifiedName(), "Type1",
            DwcTerm.measurementValue.qualifiedName(), "1.5",
            DwcTerm.measurementAccuracy.qualifiedName(), "Accurancy1",
            DwcTerm.measurementUnit.qualifiedName(), "Unit1",
            DwcTerm.measurementDeterminedBy.qualifiedName(), "By1",
            DwcTerm.measurementMethod.qualifiedName(), "Method1",
            DwcTerm.measurementRemarks.qualifiedName(), "Remarks1",
            DwcTerm.measurementDeterminedDate.qualifiedName(), "2010/2011");

    Map<String, List<Map<String, String>>> ext =
        Map.of(Extension.MEASUREMENT_OR_FACT.getRowType(), List.of(ext1));

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
