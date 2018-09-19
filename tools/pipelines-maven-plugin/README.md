# Pipelines maven plugin

Maven plugin adds new annotations [@DefaultCoder(AvroCoder.class)](https://beam.apache.org/documentation/programming-guide/#default-coders-and-the-coderregistry) and Issue interface to [avro](https://avro.apache.org/docs/current/) generated classes.

## How to use:

```xml
avro-generated-path - the path to generated classes package
avro-namespace - the namespace used in Avro schemas

<!-- Change generated avros, add GBIF features -->
<plugin>
<groupId>org.gbif.pipelines</groupId>
<artifactId>pipelines-maven-plugin</artifactId>
<executions>
  <execution>
    <goals>
      <goal>postprocess</goal>
    </goals>
    <configuration>
      <directory>${avro-generated-path}</directory>
      <defaultPackage>${avro-namespace}</defaultPackage>
    </configuration>
  </execution>
</executions>
</plugin>
```