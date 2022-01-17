package org.gbif.validator.api;

/** File type in the context of DarwinCore. */
public enum DwcFileType {
  META_DESCRIPTOR(false), // meta.xml
  METADATA(false), // eml.xml
  CORE(true),
  EXTENSION(true);

  final boolean dataBased;

  DwcFileType(boolean dataBased) {
    this.dataBased = dataBased;
  }

  /**
   * Indicates if a {@link DwcFileType} is data based or not. Data based means related to data as
   * opposed to related to metadata.
   */
  public boolean isDataBased() {
    return dataBased;
  }
}
