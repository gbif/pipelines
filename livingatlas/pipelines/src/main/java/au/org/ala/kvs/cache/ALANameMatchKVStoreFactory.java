package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.client.ALANameMatchService;
import au.org.ala.kvs.client.ALANameUsageMatch;
import au.org.ala.kvs.client.ALASpeciesMatchRequest;
import au.org.ala.kvs.client.retrofit.ALANameUsageMatchServiceClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.gbif.kvs.hbase.Command;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.factory.NameUsageMatchStoreFactory;
import org.gbif.pipelines.parsers.config.model.PipelinesConfig;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.gbif.rest.client.configuration.ClientConfiguration;
import org.gbif.rest.client.species.NameUsageMatch;

import java.io.IOException;

@Slf4j
public class ALANameMatchKVStoreFactory {

    private final KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch> kvStore;
    private static volatile ALANameMatchKVStoreFactory instance;
    private static final Object MUTEX = new Object();

    @SneakyThrows
    private ALANameMatchKVStoreFactory(ALAPipelinesConfig config) {
        this.kvStore = create(config);
    }

    public static KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch> getInstance(
            ALAPipelinesConfig config) {
        if (instance == null) {
            synchronized (MUTEX) {
                if (instance == null) {
                    instance = new ALANameMatchKVStoreFactory(config);
                }
            }
        }
        return instance.kvStore;
    }

    /**
     * Returns ala name matching key value store.
     *
     * @return
     * @throws IOException
     */
    public static KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch> create(ALAPipelinesConfig config)  {

        ClientConfiguration clientConfiguration = ClientConfiguration.builder()
                .withBaseApiUrl(config.getAlaNameMatch().getWsUrl()) //GBIF base API url
                .withTimeOut(config.getAlaNameMatch().getTimeoutSec()) //Geocode service connection time-out
                .build();

        ALANameUsageMatchServiceClient wsClient = new ALANameUsageMatchServiceClient(clientConfiguration);
        Command closeHandler = () -> {
                try {
                    wsClient.close();
                } catch (Exception e){
                    logAndThrow(e, "Unable to close");
                }
        };

        return cache2kBackedKVStore(wsClient, closeHandler, config);
    }

    /**
     * Builds a KV Store backed by the rest client.
     */
    private static KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch> cache2kBackedKVStore(ALANameMatchService nameMatchService, Command closeHandler, ALAPipelinesConfig config) {

        KeyValueStore kvs = new KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch>() {
            @Override
            public ALANameUsageMatch get(ALASpeciesMatchRequest key) {
                try {
                    return nameMatchService.match(key);
                } catch (Exception ex) {
                    throw logAndThrow(ex, "Error contacting the species match service");
                }
            }
            @Override
            public void close() throws IOException {
                closeHandler.execute();
            }
        };
        return KeyValueCache.cache(kvs, config.getAlaNameMatch().getCacheSizeMb(), ALASpeciesMatchRequest.class, ALANameUsageMatch.class);
    }

    public static SerializableSupplier<KeyValueStore<ALASpeciesMatchRequest, ALANameUsageMatch>> getInstanceSupplier(
            ALAPipelinesConfig config) {
        return () -> ALANameMatchKVStoreFactory.getInstance(config);
    }

    /**
     * Wraps an exception into a {@link RuntimeException}.
     * @param throwable to propagate
     * @param message to log and use for the exception wrapper
     * @return a new {@link RuntimeException}
     */
    private static RuntimeException logAndThrow(Throwable throwable, String message) {
        log.error(message, throwable);
        return new RuntimeException(throwable);
    }
}
