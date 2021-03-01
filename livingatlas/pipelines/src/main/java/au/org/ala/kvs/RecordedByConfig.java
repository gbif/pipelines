package au.org.ala.kvs;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class RecordedByConfig implements Serializable {

  public static final Long DEFAULT_CACHESIZEMB = 64L;

  private Long cacheSizeMb = DEFAULT_CACHESIZEMB;
}
