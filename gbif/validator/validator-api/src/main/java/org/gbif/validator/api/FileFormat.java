package org.gbif.validator.api;

/** Data file format. */
public enum FileFormat {
  DWCA(true),
  XML(false),
  TABULAR(true),
  SPREADSHEET(false);

  final boolean tabularBased;

  FileFormat(boolean tabularBased) {
    this.tabularBased = tabularBased;
  }

  public boolean isTabularBased() {
    return tabularBased;
  }
}
