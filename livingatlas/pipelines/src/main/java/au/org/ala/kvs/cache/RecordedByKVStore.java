package au.org.ala.kvs.cache;

import au.org.ala.pipelines.parser.CollectorNameParser;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.gbif.kvs.KeyValueStore;

public class RecordedByKVStore implements KeyValueStore<String, List<String>> {

  @Override
  public List<String> get(String s) {
    String[] result = CollectorNameParser.parseList(s);
    if (result != null) {
      return Arrays.asList(result);
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public void close() {
    // NOP
  }
}
