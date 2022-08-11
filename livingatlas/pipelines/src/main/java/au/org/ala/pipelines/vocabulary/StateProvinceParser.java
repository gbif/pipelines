package au.org.ala.pipelines.vocabulary;

import com.google.common.base.Strings;
import java.io.File;
import java.io.FileInputStream;
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
        InputStream is;
        if (Strings.isNullOrEmpty(dictFile)) {
          filePath = "/stateProvinces.tsv";
          is = StateProvinceParser.class.getResourceAsStream(filePath);
        } else {
          File externalFile = new File(dictFile);
          is = new FileInputStream(externalFile);
        }
        if (is == null) {
          throw new FileNotFoundException("" + filePath);
        }
        singletonObject = new StateProvinceParser(is);
      }
    }
    return singletonObject;
  }

  @Override
  protected String fromDictFile(String s) {
    return s;
  }
}
