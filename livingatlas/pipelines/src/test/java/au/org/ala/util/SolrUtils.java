package au.org.ala.util;

import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import okhttp3.internal.Util;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.codehaus.plexus.util.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities for querying SOLR outputs and for creating configsets and collections
 * in SOLR to support integration tests.
 */
@Slf4j
public class SolrUtils {

    public static final String BIOCACHE_TEST_SOLR_COLLECTION = "biocache_test";
    public static final String BIOCACHE_CONFIG_SET = "biocache_test";
    public static final String BIOCACHE_ZK_HOST = "localhost:9983";

    public static void setupIndex() throws Exception {
        try {
            deleteSolrIndex();
        } catch (Exception e){
            //expected for new setups
        }
        deleteSolrConfigSetIfExists(BIOCACHE_CONFIG_SET);
        createSolrConfigSet();
        createSolrIndex();
    }

    public static void main(String[] args) throws Exception {

        deleteSolrIndex();
        deleteSolrConfigSetIfExists(BIOCACHE_CONFIG_SET);
        createSolrConfigSet();
        createSolrIndex();
    }

    public static void createSolrConfigSet() throws Exception {
        //create a zip of

        FileUtils.forceDelete("/tmp/configset.zip");

        Map<String, String> env = new HashMap<>();
        env.put("create", "true");

        URI uri = URI.create("jar:file:/tmp/configset.zip");

        String absolutePath = new File(".").getAbsolutePath();
        String fullPath = absolutePath + "/solr/conf";

        if (!new File(fullPath).exists()){
            //try in maven land
            fullPath = new File("../solr/conf").getAbsolutePath();
        }

        Path currentDir = Paths.get(fullPath);
        FileSystem zipfs = FileSystems.newFileSystem(uri, env);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(currentDir)) {

            for (Path solrFilePath : stream) {
                if (!Files.isDirectory(solrFilePath)) {
                    Path pathInZipfile = zipfs.getPath("/" + solrFilePath.getFileName());
                    // Copy a file into the zip file
                    Files.copy(solrFilePath, pathInZipfile, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
        zipfs.close();

        OkHttpClient client = new OkHttpClient();
        MediaType MEDIA_TYPE_OCTET = MediaType.parse("application/octet-stream");

        InputStream inputStream = new FileInputStream("/tmp/configset.zip");

        RequestBody requestBody = createRequestBody(MEDIA_TYPE_OCTET, inputStream);
        Request request = new Request.Builder()
                .url("http://localhost:8983/solr/admin/configs?action=UPLOAD&name=" + BIOCACHE_CONFIG_SET)
                .post(requestBody)
                .build();

        Response response = client.newCall(request).execute();
        if (!response.isSuccessful())
            throw new IOException("Unexpected code " + response);

        log.info("POST", response.body().string());
    }

    public static void deleteSolrConfigSetIfExists(String configset) throws Exception {

        final SolrClient cloudSolrClient = new CloudSolrClient(BIOCACHE_ZK_HOST);
        final ConfigSetAdminRequest.List adminRequest = new ConfigSetAdminRequest.List();

        ConfigSetAdminResponse.List adminResponse = adminRequest.process(cloudSolrClient);

        boolean exists = adminResponse.getConfigSets().contains(configset);

        if (exists) {
            final ConfigSetAdminRequest.Delete deleteRequest = new ConfigSetAdminRequest.Delete();
            deleteRequest.setConfigSetName(configset);
            deleteRequest.process(cloudSolrClient);
        }

        cloudSolrClient.close();

    }

    public static void createSolrIndex() throws Exception {

        final SolrClient cloudSolrClient = new CloudSolrClient(BIOCACHE_ZK_HOST);
        final CollectionAdminRequest.Create adminRequest = new CollectionAdminRequest.Create();
        adminRequest.setConfigName(BIOCACHE_CONFIG_SET)
                .setCollectionName(BIOCACHE_TEST_SOLR_COLLECTION)
                .setNumShards(1)
                .setReplicationFactor(1);

        adminRequest.process(cloudSolrClient);
        cloudSolrClient.close();
    }

    public static void deleteSolrIndex() throws Exception {

        final SolrClient cloudSolrClient = new CloudSolrClient(BIOCACHE_ZK_HOST);

        final CollectionAdminRequest.List listRequest = new CollectionAdminRequest.List();
        CollectionAdminResponse response = listRequest.process(cloudSolrClient);
        NamedList<NamedList<Object>> statuses = response.getCollectionStatus();

        List<String> collections = (List) response.getResponse().get("collections");

        if(collections != null && collections.contains(BIOCACHE_TEST_SOLR_COLLECTION) ){
            final CollectionAdminRequest.Delete adminRequest = new CollectionAdminRequest.Delete();
            adminRequest.setCollectionName(BIOCACHE_TEST_SOLR_COLLECTION);
            CollectionAdminResponse adminResponse = adminRequest.process(cloudSolrClient);
        }

        cloudSolrClient.close();

    }

    public static void reloadSolrIndex() throws Exception {
        final SolrClient cloudSolrClient = new CloudSolrClient(BIOCACHE_ZK_HOST);
        final CollectionAdminRequest.Reload adminRequest = new CollectionAdminRequest.Reload();
        adminRequest.setCollectionName(BIOCACHE_TEST_SOLR_COLLECTION);
        adminRequest.process(cloudSolrClient);
        cloudSolrClient.close();
    }

    public static Long getRecordCount(String queryUrl) throws Exception {

        CloudSolrClient solr = new CloudSolrClient(BIOCACHE_ZK_HOST);
        solr.setDefaultCollection(BIOCACHE_TEST_SOLR_COLLECTION);

        SolrQuery params = new SolrQuery();
        params.setQuery(queryUrl);
        params.setSort("score ", SolrQuery.ORDER.desc);
        params.setStart(Integer.getInteger("0"));
        params.setRows(Integer.getInteger("100"));

        QueryResponse response = solr.query(params);
        SolrDocumentList results = response.getResults();
        return results.getNumFound();
    }

    public static SolrDocumentList getRecords(String queryUrl) throws Exception {

        CloudSolrClient solr = new CloudSolrClient(BIOCACHE_ZK_HOST);
        solr.setDefaultCollection(BIOCACHE_TEST_SOLR_COLLECTION);

        SolrQuery params = new SolrQuery();
        params.setQuery(queryUrl);
        params.setSort("score ", SolrQuery.ORDER.desc);
        params.setStart(Integer.getInteger("0"));
        params.setRows(Integer.getInteger("100"));

        QueryResponse response = solr.query(params);
        SolrDocumentList results = response.getResults();
        return results;
    }

    static RequestBody createRequestBody(final MediaType mediaType, final InputStream inputStream) {

        return new RequestBody() {
            @Override
            public MediaType contentType() {
                return mediaType;
            }

            @Override
            public long contentLength() {
                try {
                    return inputStream.available();
                } catch (IOException e) {
                    return 0;
                }
            }

            @Override
            public void writeTo(BufferedSink sink) throws IOException {
                Source source = null;
                try {
                    source = Okio.source(inputStream);
                    sink.writeAll(source);
                } finally {
                    Util.closeQuietly(source);
                }
            }
        };
    }
}
