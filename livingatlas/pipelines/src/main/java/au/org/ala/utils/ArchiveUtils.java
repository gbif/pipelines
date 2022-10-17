package au.org.ala.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.dwc.Archive;
import org.gbif.dwc.UnsupportedArchiveException;
import org.gbif.dwc.meta.DwcMetaFiles;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.xml.sax.SAXException;

@Slf4j
public class ArchiveUtils {

  public static final String DWCA_JSON = "dwca.json";
  public static final String META_XML = "meta.xml";

  public static String getCoreURI(InterpretationPipelineOptions options) throws IOException {

    String filePath =
        String.join(
            "/",
            options.getTargetPath(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            ValidationUtils.VERBATIM_METRICS);

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, filePath);
    Path fPath = new Path(filePath);
    if (fs.exists(fPath)) {
      log.info("Reading properties path - {}", filePath);
      try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fPath)))) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        mapper.findAndRegisterModules();
        Map<String, Object> config = mapper.readValue(br, Map.class);
        return (String) config.get("core");
      }
    }
    return null;
  }

  public static boolean isEventCore(InterpretationPipelineOptions options) {
    try {
      log.info("Core for dataset: " + getCoreURI(options));
      return DwcTerm.Event.qualifiedName().equals(getCoreURI(options));
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public static Map<String, Object> getArchiveProperties(String inputPath) throws Exception {
    Archive archive = retrieveArchive(inputPath);
    Map<String, Object> properties = new HashMap<>();
    properties.put("core", archive.getCore().getRowType().qualifiedName());
    properties.put(
        "extension",
        archive.getExtensions().stream()
            .map(ext -> ext.getRowType().qualifiedName())
            .collect(Collectors.toList()));
    return properties;
  }

  private static Archive retrieveArchive(String inputPath) throws Exception {
    boolean isDir = Paths.get(inputPath).toFile().isDirectory();
    if (isDir) {
      java.nio.file.Path dwcLocation = Paths.get(inputPath);
      java.nio.file.Path metaDescriptorFile = dwcLocation.resolve(META_XML);
      if (Files.exists(metaDescriptorFile, new LinkOption[0])) {
        try {
          return DwcMetaFiles.fromMetaDescriptor(new FileInputStream(metaDescriptorFile.toFile()));
        } catch (IOException | SAXException var4) {
          throw new UnsupportedArchiveException(var4);
        }
      }
      throw new IllegalArgumentException("Unable to access archive for path: " + inputPath);
    } else {
      return readMetaFromZip(inputPath);
    }
  }

  public static Archive readMetaFromZip(String inputPath) throws Exception {
    ZipFile zipFile = new ZipFile(inputPath);
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      if (entry.getName().equals(META_XML)) {
        InputStream stream = zipFile.getInputStream(entry);
        return DwcMetaFiles.fromMetaDescriptor(stream);
      }
    }
    throw new IllegalArgumentException("Unable to access archive for path: " + inputPath);
  }
}
