package au.org.ala.util;

import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.ingest.java.io.AvroReader;
import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.io.avro.ALAUUIDRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for querying AVRO outputs
 */
public class AvroUtils {

    public static Map<String, String> readKeysForPath(String path){

        Map<String, ALAUUIDRecord> records = AvroReader.readRecords(null, null, ALAUUIDRecord.class, path);
        Map<String, String> uniqueKeyToUuid = new HashMap<String, String>();
        for (Map.Entry<String, ALAUUIDRecord> record: records.entrySet()){
            System.out.println(record.getValue().getUniqueKey() + " -> " + record.getValue().getUuid());
            uniqueKeyToUuid.put(record.getValue().getUniqueKey(), record.getValue().getUuid());
        }
        return uniqueKeyToUuid;
    }

    public static void dumpKeysForPath(String path){
        Map<String, ALAUUIDRecord> records = AvroReader.readRecords(null, null, ALAUUIDRecord.class, path);
        Map<String, String> uniqueKeyToUuid = new HashMap<String, String>();
        for (Map.Entry<String, ALAUUIDRecord> record: records.entrySet()){
            System.out.println(record.getValue().getUniqueKey() + " -> " + record.getValue().getUuid());
        }
    }
}
