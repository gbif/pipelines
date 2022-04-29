package au.org.ala.pipelines.beam;

import au.com.bytecode.opencsv.CSVParser;
import au.org.ala.utils.ALAFsUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.common.PipelinesException;

public class ImagePipelineUtils {

  public static void validateHeaders(List<String> headers, List<String> requiredHeaders) {
    for (String hdr : requiredHeaders) {
      if (!headers.contains(hdr.toLowerCase(Locale.ROOT))) {
        throw new PipelinesException("Missing header: " + hdr);
      }
    }
  }

  public static int indexOf(List<String> headers, String header) {
    return headers.indexOf(header.toLowerCase(Locale.ROOT));
  }

  public static int indexOf(List<String> headers, Term term) {
    return headers.indexOf(term.simpleName().toLowerCase(Locale.ROOT));
  }

  /**
   * Read the headers, returning them lowercased
   *
   * @param fs
   * @param imageServiceExportPath
   * @return
   * @throws IOException
   */
  public static List<String> readHeadersLowerCased(FileSystem fs, String imageServiceExportPath)
      throws IOException {

    InputStream input = ALAFsUtils.openInputStream(fs, imageServiceExportPath);
    GZIPInputStream inputStream = new GZIPInputStream(input);
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String headerLine = reader.readLine();
    final CSVParser parser = new CSVParser();
    List<String> headers =
        Arrays.stream(parser.parseLine(headerLine))
            .map(c -> c.toLowerCase(Locale.ROOT))
            .collect(Collectors.toList());
    reader.close();
    return headers;
  }
}
