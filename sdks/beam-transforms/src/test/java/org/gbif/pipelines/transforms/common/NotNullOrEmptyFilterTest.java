package org.gbif.pipelines.transforms.common;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class NotNullOrEmptyFilterTest {

  @Test
  public void notNullOrEmptyFilterTest() {

    //Test records
    ExtendedRecord withParentCoredId = ExtendedRecord.newBuilder().setId("1").setParentCoreId("1").build();
    ExtendedRecord withOutParentCoredId = ExtendedRecord.newBuilder().setId("1").build();
    List<ExtendedRecord> verbatimRecords = Arrays.asList(withParentCoredId, withOutParentCoredId);

    //Filter using Beam function
    Optional<ExtendedRecord> filteredRecord =
    verbatimRecords.stream().filter(vr -> NotNullOrEmptyFilter.of(ExtendedRecord::getParentCoreId).apply(vr)).findFirst();

    //The expected records is retrieve after being filtered
    assertTrue(filteredRecord.isPresent());
    assertEquals(withParentCoredId, filteredRecord.get());
  }
}
