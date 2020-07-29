package au.org.ala.util;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.ingest.java.io.AvroReader;
import org.gbif.pipelines.io.avro.ALAUUIDRecord;

/** Utilities for querying AVRO outputs */
@Slf4j
public class AvroUtils {

  public static Map<String, String> readKeysForPath(String path) {

    Map<String, ALAUUIDRecord> records =
        AvroReader.readRecords(null, null, ALAUUIDRecord.class, path);
    Map<String, String> uniqueKeyToUuid = new HashMap<String, String>();
    for (Map.Entry<String, ALAUUIDRecord> record : records.entrySet()) {
      log.debug(record.getValue().getUniqueKey() + " -> " + record.getValue().getUuid());
      uniqueKeyToUuid.put(record.getValue().getUniqueKey(), record.getValue().getUuid());
    }
    return uniqueKeyToUuid;
  }
}
