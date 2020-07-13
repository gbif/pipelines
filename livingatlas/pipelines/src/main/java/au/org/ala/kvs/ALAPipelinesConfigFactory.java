package au.org.ala.kvs;

import au.org.ala.utils.ALAFsUtils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.SneakyThrows;

public class ALAPipelinesConfigFactory {

    private static volatile ALAPipelinesConfigFactory instance;

    private final ALAPipelinesConfig config;

    private static final Object MUTEX = new Object();

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    static {
        MAPPER.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        MAPPER.findAndRegisterModules();
    }

    @SneakyThrows
    private ALAPipelinesConfigFactory(String hdfsSiteConfig, String coreSiteConfig, String propertiesPath) {
        this.config = ALAFsUtils.readConfigFile(hdfsSiteConfig, coreSiteConfig, propertiesPath);
    }

    public static ALAPipelinesConfigFactory getInstance(String hdfsSiteConfig,String coreSiteConfig, String propertiesPath) {
        if (instance == null) {
            synchronized (MUTEX) {
                if (instance == null) {
                    instance = new ALAPipelinesConfigFactory(hdfsSiteConfig, coreSiteConfig, propertiesPath);
                }
            }
        }
        return instance;
    }

    public ALAPipelinesConfig get() {
        return config;
    }
}
