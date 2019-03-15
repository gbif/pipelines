package org.gbif.pipeleins.api;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Wraps the result of looking up an Occurrence key in order to provide information on whether the key was created for
 * this request or not.
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class KeyLookupResult {

  private final int key;
  private final boolean created;

}
