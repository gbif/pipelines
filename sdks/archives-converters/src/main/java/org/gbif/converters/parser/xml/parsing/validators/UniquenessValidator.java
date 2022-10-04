package org.gbif.converters.parser.xml.parsing.validators;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

/**
 * Validates the uniqueness of the String IDs passed using a map backed by a temp file. It uses the
 * mapdb library to implement the map (http://www.mapdb.org/).
 *
 * <p>This class is intended to be used per process that needs this validation, so a new instance
 * has to be created each time. At the moment of creating the instance, a {@link DB} backed by a
 * temp file is created.
 *
 * <p>Keep in mind that {@link UniquenessValidator#close()} has to be called when finishing the
 * validation in order to release the resources used. Also notice that the class implements the
 * {@link AutoCloseable} interface.
 */
public class UniquenessValidator implements AutoCloseable {

  private final DB dbDisk;
  private final HTreeMap.KeySet<String> setOnDisk;

  private UniquenessValidator() {

    // create database in disk.
    // The fileChannelEnable is set to be used only when mmap is not supported (mapdb does it
    // internally).
    // cleanerHackEnable is a workaround for a JVM bug
    // (https://jankotek.gitbooks.io/mapdb/content/performance/).
    dbDisk =
        DBMaker.tempFileDB()
            .fileMmapEnableIfSupported()
            .cleanerHackEnable()
            .fileChannelEnable()
            .make();

    // prefix name to create the map
    long time = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);

    // map to store the values
    setOnDisk = dbDisk.hashSet(time + "-disk").serializer(Serializer.STRING).createOrOpen();
  }

  /**
   * Creates a new instance.
   *
   * @return {@link UniquenessValidator}
   */
  public static UniquenessValidator getNewInstance() {
    return new UniquenessValidator();
  }

  /**
   * Validates that the ID received is unique in the map that backs this class.
   *
   * @param id ID to validate. This parameter is required and cannot be null.
   * @return true if the ID is unique, false otherwise
   */
  public boolean isUnique(String id) {
    Objects.requireNonNull(id, "ID is required");
    // this set returns false when a value is inserted
    return setOnDisk.add(id);
  }

  @Override
  public void close() {
    if (!dbDisk.isClosed()) {
      dbDisk.close();
    }
  }
}
