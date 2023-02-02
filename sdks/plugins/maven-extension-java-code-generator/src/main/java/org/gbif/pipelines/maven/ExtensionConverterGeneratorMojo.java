package org.gbif.pipelines.maven;

import static java.nio.charset.StandardCharsets.UTF_8;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.digester.ThesaurusHandlingRule;
import org.gbif.dwc.extensions.ExtensionFactory;
import org.gbif.dwc.extensions.VocabulariesManager;
import org.gbif.dwc.extensions.Vocabulary;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.xml.SAXUtils;
import org.gbif.pipelines.maven.ExtensionPojo.Setter;

@Mojo(name = "extensiongeneration", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class ExtensionConverterGeneratorMojo extends AbstractMojo {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private static final String POSTFIX_CLASS_NAME = "Converter.java";

  @Parameter(property = "extensiongeneration.pathToWrite")
  private String pathToWrite;

  @Parameter(property = "extensiongeneration.packagePath")
  private String packagePath;

  @Parameter(property = "extensiongeneration.avroNamespace")
  private String avroNamespace;

  @Parameter(property = "extensiongeneration.extensionPackage", defaultValue = "extension")
  private String extensionPackage;

  @Override
  public void execute() throws MojoExecutionException {

    try {

      String finalPath = String.join("/", pathToWrite, packagePath.replace('.', '/'));
      Files.createDirectories(Paths.get(finalPath));
      Configuration cfg = createConfig();

      Map<Extension, String> extensions = Extension.availableExtensionResources();

      for (Entry<Extension, String> extension : extensions.entrySet()) {
        try {
          URL url = new URL(extension.getValue());
          String name = normalizeClassNmae(extension.getKey().name());

          Path classPath = Paths.get(finalPath, name + POSTFIX_CLASS_NAME);
          if (!Files.exists(classPath)) {
            createAndWrite(name, url, classPath, cfg);
          }
        } catch (Exception ex) {
          getLog().warn(ex.getMessage());
        }
      }

    } catch (IOException ex) {
      throw new MojoExecutionException(ex.getMessage());
    }
  }

  public void setPathToWrite(String pathToWrite) {
    this.pathToWrite = pathToWrite;
  }

  public void setAvroNamespace(String avroNamespace) {
    this.avroNamespace = avroNamespace;
  }

  public void setPackagePath(String packagePath) {
    this.packagePath = packagePath;
  }

  /** Create a java class file using Freemarker template */
  private void createAndWrite(String name, URL url, Path classPath, Configuration cfg)
      throws Exception {
    org.gbif.dwc.extensions.Extension dwcExt = getExtension(url);

    Map<String, Integer> duplicatesMap = new HashMap<>();
    dwcExt
        .getProperties()
        .forEach(
            x -> {
              if (duplicatesMap.containsKey(x.getName())) {
                duplicatesMap.computeIfPresent(x.getName(), (s, i) -> ++i);
              } else {
                duplicatesMap.put(x.getName(), 1);
              }
            });

    List<Setter> setters =
        dwcExt.getProperties().stream()
            .map(
                e -> {
                  String eName = e.getName();
                  if (duplicatesMap.get(eName) > 1) {
                    eName = TERM_FACTORY.findTerm(e.qualifiedName()).prefixedName();
                  }
                  return new Setter(e.getQualname(), normalizeName(eName));
                })
            .collect(Collectors.toList());

    String[] extraNamespace =
        url.toString().replaceAll("http://rs.gbif.org/extension/", "").split("/");

    ExtensionPojo extPojo =
        new ExtensionPojo(
            name,
            avroNamespace,
            packagePath,
            dwcExt.getRowType().qualifiedName(),
            setters,
            extensionPackage + "." + extraNamespace[0]);

    Template temp =
        new Template("table-converter", new StringReader(Templates.TABLE_CONVERTER), cfg);

    Writer out = new OutputStreamWriter(Files.newOutputStream(classPath.toFile().toPath()), UTF_8);
    temp.process(extPojo, out);

    getLog().info("DWC extension java converter class is generated - " + classPath);
  }

  /** Create Freemarker config */
  private Configuration createConfig() {
    Configuration cfg = new Configuration(Configuration.VERSION_2_3_31);
    cfg.setDefaultEncoding(UTF_8.toString());
    cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    cfg.setLogTemplateExceptions(false);
    cfg.setWrapUncheckedExceptions(true);
    cfg.setFallbackOnNullLoopVariable(false);
    cfg.setTabSize(1);
    return cfg;
  }

  private String normalizeClassNmae(String name) {
    return Arrays.stream(name.split("_"))
            .map(String::toLowerCase)
            .map(x -> x.substring(0, 1).toUpperCase() + x.substring(1))
            .collect(Collectors.joining())
        + "Table";
  }

  /** Read DWC extension from URL */
  private org.gbif.dwc.extensions.Extension getExtension(URL url) throws Exception {
    ThesaurusHandlingRule thr = new ThesaurusHandlingRule(new EmptyVocabulariesManager());
    ExtensionFactory factory = new ExtensionFactory(thr, SAXUtils.getNsAwareSaxParserFactory());
    return factory.build(url.openStream(), url, false);
  }

  /** Normalize name and retun result like - Normalizename */
  protected String normalizeName(String name) {
    String replace = name.toLowerCase().trim().replace("-", "").replace("_", "");
    return replace.substring(0, 1).toUpperCase() + replace.substring(1);
  }

  private static class EmptyVocabulariesManager implements VocabulariesManager {

    @Override
    public Vocabulary get(String uri) {
      return null;
    }

    @Override
    public Vocabulary get(URL url) {
      return null;
    }

    @Override
    public Map<String, String> getI18nVocab(String uri, String lang) {
      return null;
    }

    @Override
    public List<Vocabulary> list() {
      return null;
    }
  }
}
