package au.org.ala.pipelines.vocabulary;

import java.io.FileNotFoundException;
import java.io.InputStream;
import org.gbif.common.parsers.core.FileBasedDictionaryParser;

public class StateProvinceParser extends FileBasedDictionaryParser<String> {

  private static StateProvinceParser singletonObject = null;

  private StateProvinceParser(InputStream file) {
    super(false);
    init(file);
  }

  public static StateProvinceParser getInstance(String dictFile) throws FileNotFoundException {
    synchronized (StateProvinceParser.class) {
      if (singletonObject == null) {
        String filePath = dictFile;
        if (dictFile == null) {
          filePath = "/stateProvinces.tsv";
        }
        InputStream in = StateProvinceParser.class.getResourceAsStream(filePath);
        if (in == null) {
          throw new FileNotFoundException("" + filePath);
        }
        singletonObject = new StateProvinceParser(in);
      }
    }
    return singletonObject;
  }

  @Override
  protected String fromDictFile(String s) {
    return s;
  }
}
