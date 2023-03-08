package au.org.ala.util;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.utils.ALAFsUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
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

  // public static final String NAME_SERVICE_IMG =
  // "charvolant/ala-namematching-service:v20200214-6";
  public static final String NAME_SERVICE_IMG =
      "atlasoflivingaustralia/ala-namematching-service:v20210811-3";
  public static final String SENSTIVE_SERVICE_IMG =
      "charvolant/ala-sensitive-data-service:v20200214-3";
  public static final String SOLR_IMG = "atlasoflivingaustralia/solr8-jts";

  public static final int NAME_SERVICE_INTERNAL_PORT = 9179;
  public static final int SENSITIVE_SERVICE_INTERNAL_PORT = 9189;

  public static final int ES_INTERNAL_PORT = 9200;

  public static ALAPipelinesConfig getConfig() {
    String absolutePath = new File(getPipelinesConfigFile()).getAbsolutePath();
    return ALAFsUtils.readConfigFile(HdfsConfigs.nullConfig(), absolutePath);
  }

  public static String getPipelinesConfigFile() {
    try {
      String templateFile = "src/test/resources/pipelines.yaml";

      // read file into string
      String template =
          FileUtils.readFileToString(new File(templateFile), Charset.forName("UTF-8"));
      template = template.replaceAll("SOLR_PORT", System.getProperty("SOLR_PORT", "8983"));
      template =
          template.replaceAll(
              "ALA_NAME_MATCH_PORT",
              System.getProperty(
                  "ALA_NAME_MATCH_PORT", String.valueOf(NAME_SERVICE_INTERNAL_PORT)));
      template =
          template.replaceAll(
              "SDS_PORT",
              System.getProperty("SDS_PORT", String.valueOf(SENSITIVE_SERVICE_INTERNAL_PORT)));
      template =
          template.replaceAll("COLLECTORY_PORT", System.getProperty("COLLECTORY_PORT", "3939"));
      template = template.replaceAll("LISTS_PORT", System.getProperty("LISTS_PORT", "4949"));
      template = template.replaceAll("SPATIAL_PORT", System.getProperty("SPATIAL_PORT", "5959"));
      template =
          template.replaceAll(
              "ES_PORT", System.getProperty("ES_PORT", String.valueOf(ES_INTERNAL_PORT)));

      Path tempFile = Files.createTempFile(null, ".yaml");
      Files.write(tempFile, template.getBytes(), StandardOpenOption.CREATE);
      tempFile.toFile().deleteOnExit();
      return tempFile.toString();
    } catch (Exception e) {
      throw new RuntimeException("Unable to read template", e);
    }
  }

  public static int getFreePort() {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      serverSocket.setReuseAddress(true);
      int port = serverSocket.getLocalPort();
      try {
        serverSocket.close();
      } catch (IOException e) {
        // Ignore IOException on close()
      }
      return port;
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  public static int[] getFreePortsForSolr() {

    int retries = 5;
    for (int i = 0; i < retries; i++) {
      try {
        ServerSocket serverSocket = new ServerSocket(0);
        serverSocket.setReuseAddress(true);
        int port = serverSocket.getLocalPort();
        ServerSocket serverSocket2 = new ServerSocket(port + 1000);
        serverSocket2.setReuseAddress(true);
        serverSocket2.close();
        return new int[] {port, port + 1000};
      } catch (Exception e) {
        retries++;
      }
    }
    throw new RuntimeException("Unable to find free socket pair after no of retries: " + retries);
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

  public static MockWebServer createMockSpatialServer() {
    MockWebServer server = new MockWebServer();
    final Dispatcher dispatcher =
        new Dispatcher() {
          @Override
          public MockResponse dispatch(RecordedRequest request) {

            try {

              if (request.getPath().endsWith("distribution/")) {
                String responseBody =
                    FileUtils.readFileToString(
                        new File("src/test/resources/sampling-service/distributions.json"),
                        "UTF-8");
                return new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);
              }

              if (request.getPath().startsWith("/distribution/lsids/")) {
                String responseBody =
                    FileUtils.readFileToString(
                        new File("src/test/resources/sampling-service/grey-nurse.json"), "UTF-8");
                return new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);
              }

              if (request
                  .getPath()
                  .startsWith(
                      "/distribution/outliers/https%253A%252F%252Fbiodiversity.org.au%252Fafd%252Ftaxa%252F0c3e2403-05c4-4a43-8019-30e6d657a283")) {
                String responseBody =
                    FileUtils.readFileToString(
                        new File("src/test/resources/sampling-service/multi-value.json"), "UTF-8");
                return new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);

              } else if (request.getPath().startsWith("/distribution/outliers/")) {
                String responseBody =
                    FileUtils.readFileToString(
                        new File("src/test/resources/sampling-service/empty-response.json"),
                        "UTF-8");
                return new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);
              }

              // layers...
              if (request.getPath().endsWith("/layers")) {
                String responseBody =
                    FileUtils.readFileToString(
                        new File("src/test/resources/sampling-service/layers.json"), "UTF-8");
                return new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);
              }

              // sampling
              if (request.getPath().endsWith("/fields")) {
                String responseBody =
                    FileUtils.readFileToString(
                        new File("src/test/resources/sampling-service/fields.json"), "UTF-8");
                return new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody(responseBody);
              }

              if (request.getPath().endsWith("/intersect/batch")) {

                return new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody("{ \"batchId\": 12345678}");
              }

              if (request.getPath().startsWith("/intersect/batch/")) {
                return new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody(
                        "{\"status\": \"finished\", \"downloadUrl\": \""
                            + getConfig().getSamplingService().getWsUrl()
                            + "/download/sampling\"}");
              }

              if (request.getPath().startsWith("/download/sampling")) {
                File f = File.createTempFile("12345678", "zip");
                ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
                ZipEntry e = new ZipEntry("12345678.csv");
                out.putNextEntry(e);
                byte[] data =
                    FileUtils.readFileToString(
                            new File("src/test/resources/sampling-service/sampling-output.csv"),
                            "UTF-8")
                        .getBytes();
                out.write(data, 0, data.length);
                out.closeEntry();
                out.close();

                Source source = Okio.source(f);
                Buffer buffer = new Buffer();
                buffer.writeAll(source);
                return new MockResponse()
                    .setHeader("contentType", "text/csv")
                    .setHeader("Content-Disposition", "attachment;filename=sampling.zip")
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

  public static MockWebServer startMockSpatialServer() {
    try {
      int freePort = getFreePort();
      System.setProperty("SPATIAL_PORT", "" + freePort);
      MockWebServer s = createMockSpatialServer();
      s.start(freePort);
      return s;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static MockWebServer startMockCollectoryServer() {
    try {
      int freePort = getFreePort();
      System.setProperty("COLLECTORY_PORT", String.valueOf(freePort));
      MockWebServer s = createMockCollectory();
      s.start(freePort);
      return s;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static MockWebServer startSpeciesListServer() {
    try {
      int freePort = getFreePort();
      System.setProperty("LISTS_PORT", String.valueOf(freePort));
      MockWebServer s = createMockSpeciesLists();
      s.start(freePort);
      return s;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static MockWebServer createMockCollectory() {
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

  public static void setSolrPorts(int solrPort, int zkPort) {
    System.setProperty("SOLR_PORT", String.valueOf(solrPort));
    System.setProperty("ZK_PORT", String.valueOf(zkPort));
  }

  public static void setNameServicePort(Integer mappedPort) {
    System.setProperty("ALA_NAME_MATCH_PORT", String.valueOf(mappedPort));
  }

  public static void setSDSPort(Integer mappedPort) {
    System.setProperty("SDS_PORT", String.valueOf(mappedPort));
  }

  public static void setESPort(Integer mappedPort) {
    System.setProperty("ES_PORT", String.valueOf(mappedPort));
  }
}
