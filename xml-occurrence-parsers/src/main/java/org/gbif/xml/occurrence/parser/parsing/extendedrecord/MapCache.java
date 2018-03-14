package org.gbif.xml.occurrence.parser.parsing.extendedrecord;

import org.gbif.xml.occurrence.parser.model.RawOccurrenceRecord;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

public class MapCache {

  private final DB db;
  private final HTreeMap.KeySet<String> set;

  private MapCache() {
    this.db = DBMaker.memoryDB().make();
    long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    this.set = db.hashSet(String.valueOf(time)).serializer(Serializer.STRING).createOrOpen();
  }

  private static class LazyHolder {
    static final MapCache INSTANCE = new MapCache();
  }

  public static MapCache getInstance() {
    return LazyHolder.INSTANCE;
  }

  public boolean isUnique(RawOccurrenceRecord rawOccurrenceRecord) {
    Objects.requireNonNull(rawOccurrenceRecord.getId(), "All records are required to have an ID [Hint: see XML parsers]");
    return set.add(rawOccurrenceRecord.getId());
  }

  public void close() {
    // TODO: DOES IT DELETE FILE?
    if(!db.isClosed()) {
      db.close();
    }
  }

}




