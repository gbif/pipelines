package au.org.ala.util;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.zip.GZIPOutputStream;
import lombok.extern.slf4j.Slf4j;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import okio.Okio;
import okio.Source;
import org.apache.commons.io.FileUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;

@Slf4j
public class TestUtils {

  public static ALAPipelinesConfig getConfig() {
    String absolutePath = new File(getPipelinesConfigFile()).getAbsolutePath();
    return ALAPipelinesConfigFactory.getInstance(HdfsConfigs.nullConfig(), absolutePath).get();
  }

  public static String getPipelinesConfigFile() {
    return System.getProperty("pipelinesTestYamlConfigFile", "src/test/resources/pipelines.yaml");
  }

  public static int getCollectoryPort() throws Exception {
    String urlStr = TestUtils.getConfig().getCollectory().getWsUrl();
    URL url = new URL(urlStr);
    return url.getPort();
  }

  public static int getSpeciesListPort() throws Exception {
    String urlStr = TestUtils.getConfig().getSpeciesListService().getWsUrl();
    URL url = new URL(urlStr);
    return url.getPort();
  }

  public static MockWebServer createMockSpeciesLists() {
    MockWebServer server = new MockWebServer();
    final Dispatcher dispatcher =
        new Dispatcher() {
          @Override
          public MockResponse dispatch(RecordedRequest request) {

            try {
              // authoritative lists
              if (request.getPath().startsWith("/ws/speciesList")) {
                String responseBody =
                    FileUtils.readFileToString(
                        new File("src/test/resources/species-lists/list.json"), "UTF-8");
                return new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);
              }

              // list download
              if (request.getPath().startsWith("/speciesListItem/downloadList/dr1")) {

                Source source =
                    Okio.source(new File("src/test/resources/species-lists/test-list.csv"));
                Buffer buffer = new Buffer();
                buffer.writeAll(source);
                return new MockResponse()
                    .setHeader("contentType", "text/csv")
                    .setHeader("Content-Disposition", "attachment;filename=test-list.csv")
                    .setResponseCode(200)
                    .setBody(buffer);
              }

              // list download
              if (request.getPath().startsWith("/speciesListItem/downloadList/dr2")) {

                Source source =
                    Okio.source(new File("src/test/resources/species-lists/test-list2.csv"));
                Buffer buffer = new Buffer();
                buffer.writeAll(source);
                return new MockResponse()
                    .setHeader("contentType", "text/csv")
                    .setHeader("Content-Disposition", "attachment;filename=test-list.csv")
                    .setResponseCode(200)
                    .setBody(buffer);
              }

              // list download
              if (request.getPath().startsWith("/speciesListItem/downloadList/dr3")) {
                Source source = Okio.source(new File("src/test/resources/species-lists/griis.csv"));
                Buffer buffer = new Buffer();
                buffer.writeAll(source);
                return new MockResponse()
                    .setHeader("contentType", "text/csv")
                    .setHeader("Content-Disposition", "attachment;filename=test-list.csv")
                    .setResponseCode(200)
                    .setBody(buffer);
              }
            } catch (Exception e) {
              return new MockResponse().setResponseCode(500);
            }
            return new MockResponse().setResponseCode(400);
          }
        };
    server.setDispatcher(dispatcher);
    return server;
  }

  public static MockWebServer createMockCollectory() {
    MockWebServer server = new MockWebServer();
    final Dispatcher dispatcher =
        new Dispatcher() {
          @Override
          public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
            try {

              if (request.getPath().equalsIgnoreCase("/lookup/inst/CSIROCXXX/coll/ANIC")) {
                return new MockResponse().setResponseCode(400);
              }

              if (request.getPath().equalsIgnoreCase("/lookup/inst/CSIRO/coll/ANIC")) {
                File absolutePath = new File("src/test/resources/collectory/ANIC.json");
                String responseBody = FileUtils.readFileToString(absolutePath, "UTF-8");
                return new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);
              }

              String datasetID =
                  request.getPath().substring(request.getPath().lastIndexOf('/') + 1);
              // read src
              File absolutePath = new File("src/test/resources/collectory/" + datasetID + ".json");

              if (!absolutePath.exists()) {
                return new MockResponse().setResponseCode(404);
              }

              String responseBody = FileUtils.readFileToString(absolutePath, "UTF-8");
              return new MockResponse()
                  .setResponseCode(200)
                  .setHeader("Content-Type", "application/json")
                  .setBody(responseBody);
            } catch (Exception e) {
              log.error(e.getMessage(), e);
              throw new InterruptedException(e.getMessage());
            }
          }
        };
    server.setDispatcher(dispatcher);
    return server;
  }

  public static void compressGzip(String source, String target) throws IOException {

    try (GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(target));
        FileInputStream fis = new FileInputStream(source)) {
      // copy file
      byte[] buffer = new byte[1024];
      int len;
      while ((len = fis.read(buffer)) > 0) {
        gos.write(buffer, 0, len);
      }
    }
  }
}
