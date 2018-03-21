package org.gbif.xml.occurrence.parser.parsing.extendedrecord;

import org.gbif.xml.occurrence.parser.model.RawOccurrenceRecord;

public class MapCache {

  private MapCache() {
  }

  private static class LazyHolder {
    static final MapCache INSTANCE = new MapCache();
  }

  public static MapCache getInstance() {
    return LazyHolder.INSTANCE;
  }

  public boolean isUnique(RawOccurrenceRecord rawOccurrenceRecord) {
    return true;
  }

  public void create() {
  }

  public void close() {
  }

}