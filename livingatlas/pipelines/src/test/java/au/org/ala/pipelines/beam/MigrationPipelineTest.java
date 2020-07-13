package au.org.ala.pipelines.beam;

import au.org.ala.util.AvroUtils;
import org.codehaus.plexus.util.FileUtils;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MigrationPipelineTest {

    @Test
    public void testMigration() throws Exception {

        FileUtils.forceDelete(new File("/tmp/la-pipelines-test/uuid-migration"));

        String absolutePath = new File("src/test/resources").getAbsolutePath();
        InterpretationPipelineOptions options = PipelinesOptionsFactory.create(InterpretationPipelineOptions.class, new String[]{
                "--datasetId=ALL",
                "--attempt=1",
                "--runner=DirectRunner",
                "--metaFileName=/tmp/la-pipelines-test/uuid-migration/migration-metrics.yml",
                "--targetPath=/tmp/la-pipelines-test/uuid-migration",
                "--inputPath=" + absolutePath + "/uuid-migration/occ_uuid.csv",
                "--properties=src/test/resources/pipelines.yaml"
        });
        MigrateUUIDPipeline.run(options);

        Map<String, String> dr1 = AvroUtils.readKeysForPath("/tmp/la-pipelines-test/uuid-migration/dr1/1/identifiers/ala_uuid/interpret-*");
        assertEquals( 4, dr1.size());

        Map<String, String> dr2 = AvroUtils.readKeysForPath("/tmp/la-pipelines-test/uuid-migration/dr2/1/identifiers/ala_uuid/interpret-*");
        assertEquals( 1, dr2.size());

        Map<String, String> dr3 = AvroUtils.readKeysForPath("/tmp/la-pipelines-test/uuid-migration/dr3/1/identifiers/ala_uuid/interpret-*");
        assertEquals( 1, dr3.size());
    }
}
