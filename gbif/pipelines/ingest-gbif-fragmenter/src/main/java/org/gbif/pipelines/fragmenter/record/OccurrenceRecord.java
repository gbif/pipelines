package org.gbif.pipelines.fragmenter.record;

public interface OccurrenceRecord {

  String toStringRecord();

  String getInstitutionCode();

  String getCollectionCode();

  String getCatalogNumber();

  String getOccurrenceId();
}
