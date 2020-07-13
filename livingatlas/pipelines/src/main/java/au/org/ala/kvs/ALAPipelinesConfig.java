package au.org.ala.kvs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.parsers.config.model.*;

import java.io.Serializable;

/**
 * Living Atlas configuration extensions
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ALAPipelinesConfig implements Serializable {

    PipelinesConfig gbifConfig;

    //ALA specific
    private WsConfig collectory;
    private WsConfig alaNameMatch;
    private WsConfig lists;

    GeocodeShpConfig geocodeConfig;
    LocationInfoConfig locationInfoConfig;
}
