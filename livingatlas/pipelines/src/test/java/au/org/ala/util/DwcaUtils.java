package au.org.ala.util;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.gbif.pipelines.core.io.DwcaReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/** Test utilities for reading darwin core archives for testing the outputs of tests. */
public class DwcaUtils {

  public static long countRecordsInCore(String pathToZip) throws Exception {
    File tmpdir = Files.createTempDirectory("tmp-la-pipelines-test").toFile();
    DwcaReader reader = DwcaReader.fromCompressed(pathToZip, tmpdir.getAbsolutePath());
    long count = 0;
    while (reader.advance()) {
      count++;
    }
    return count;
  }

  public static long countRecordsInExtension(String pathToZip, String extension) throws Exception {
    File tmpdir = Files.createTempDirectory("tmp-la-pipelines-test").toFile();
    DwcaReader reader = DwcaReader.fromCompressed(pathToZip, tmpdir.getAbsolutePath());
    long count = 0;
    while (reader.advance()) {
      ExtendedRecord er = reader.getCurrent();
      List<Map<String, String>> extensionRecords = er.getExtensions().get(extension);
      count = count + extensionRecords.stream().count();
    }
    reader.close();
    return count;
  }
}
