package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.client.*;
import au.org.ala.kvs.client.retrofit.ALACollectoryServiceClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.gbif.kvs.hbase.Command;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.gbif.rest.client.configuration.ClientConfiguration;

import java.io.IOException;

@Slf4j
public class ALACollectionKVStoreFactory {

    private final KeyValueStore<ALACollectionLookup, ALACollectionMatch> kvStore;
    private static volatile ALACollectionKVStoreFactory instance;
    private static final Object MUTEX = new Object();

    @SneakyThrows
    private ALACollectionKVStoreFactory(ALAPipelinesConfig config) {
        this.kvStore = create(config);
    }

    public static KeyValueStore<ALACollectionLookup, ALACollectionMatch> getInstance(
            ALAPipelinesConfig config) {
        if (instance == null) {
            synchronized (MUTEX) {
                if (instance == null) {
                    instance = new ALACollectionKVStoreFactory(config);
                }
            }
        }
        return instance.kvStore;
    }

    /**
     *
     * @return
     * @throws IOException
     */
    public static KeyValueStore<ALACollectionLookup, ALACollectionMatch> create(ALAPipelinesConfig config) {

        ClientConfiguration clientConfiguration = ClientConfiguration.builder()
                .withBaseApiUrl(config.getCollectory().getWsUrl()) //GBIF base API url
                .withTimeOut(config.getCollectory().getTimeoutSec()) //Geocode service connection time-out
                .build();

        ALACollectoryServiceClient wsClient = new ALACollectoryServiceClient(clientConfiguration);
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
    private static KeyValueStore<ALACollectionLookup, ALACollectionMatch> cache2kBackedKVStore(ALACollectoryService service, Command closeHandler, ALAPipelinesConfig config) {

        KeyValueStore kvs = new KeyValueStore<ALACollectionLookup, ALACollectionMatch>() {
            @Override
            public ALACollectionMatch get(ALACollectionLookup key) {
                try {
                    return service.lookupCodes(key.getInstitutionCode(), key.getCollectionCode());
                } catch (Exception ex) {
                    //this is can happen for bad data and this service is suspectible to http 404 due to the fact
                    // it takes URL parameters from the raw data. So log and carry on for now.
                    log.error("Error contacting the collectory service with institutionCode {} and collectionCode {} Message: {}",
                            key.getInstitutionCode(),
                            key.getCollectionCode(),
                            ex.getMessage(),
                            ex);
                }
                return ALACollectionMatch.EMPTY;
            }

            @Override
            public void close() throws IOException {
                closeHandler.execute();
            }
        };
        return KeyValueCache.cache(kvs, config.getCollectory().getCacheSizeMb(), ALACollectionLookup.class, ALACollectionMatch.class);
    }

    public static SerializableSupplier<KeyValueStore<ALACollectionLookup, ALACollectionMatch>> getInstanceSupplier(
            ALAPipelinesConfig config) {
        return () -> ALACollectionKVStoreFactory.getInstance(config);
    }

    /**
     * Wraps an exception into a {@link RuntimeException}.
     *
     * @param throwable to propagate
     * @param message to log and use for the exception wrapper
     * @return a new {@link RuntimeException}
     */
    private static RuntimeException logAndThrow(Throwable throwable, String message) {
        log.error(message, throwable);
        return new RuntimeException(throwable);
    }
}
