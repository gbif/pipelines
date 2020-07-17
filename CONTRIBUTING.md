# Contributing to GBIF Pipelines

The GBIF Pipelines development community welcomes contributions from anyone! Thank you.

If you have questions please open [an issue](https://github.com/gbif/pipelines/issues/new) or reach out to the developers starting with informatics@gbif.org.

There are different ways you can contribute that heklp this project:
- Log [issues](https://github.com/gbif/pipelines/issues) and help document a bug or specify new functionality 
- Improve documentation
- Provide code submissions that address issues or brings new functionalities
- Help test the functionalities offered in the project
- Help improve this guide 

## Contributing code

Below is a tutorial for contributing code to Pipelines, covering our tools and typical process in detail.

### Prerequisites

To contribute code, you need

- a GitHub account
- a Linux, macOS, or Microsoft Windows development environment with Java JDK 8 installed
- Maven (version 3.6+)

### Connect with the Pipeline community and share your intent

- This is a very active project, with dependencies coming from those running in production.
- It is always worthwhile announcing your intention, so the community can offer guidance. 
- Please always start with an issue to capture discussion    

### Development setup

#### Command line

This project uses [Apache maven](https://maven.apache.org/run.html) to build. The following should be enough to checkout and build the project.

```
git clone git@github.com:gbif/pipelines.git
cd pipelines
mvn clean package
```
To skip integration test, `mvn clean package -DskipITs` can be run or `mvn clean package -DskipITs`. 

We use [Google Java Format](https://plugins.jetbrains.com/plugin/8527-google-java-format) for code styling. 

From the command line you can check the style using `mvn spotless:check` and fixup styling issues using `mvn spotless:apply`.

#### IDE Setup  

We recommend the following toolset, and are able to answer questions on this configuration:

- Use [Intellij IDEA Community](https://www.jetbrains.com/idea/download/) (or better)
- Use [Google Java Format](https://plugins.jetbrains.com/plugin/8527-google-java-format) (Please, do not reformat old codebase, only new)
- The project uses [Project Lombok](https://projectlombok.org/), please install [Lombok plugin for Intellij IDEA](https://plugins.jetbrains.com/plugin/6317-lombok-plugin).
- Because the project uses [Error-prone](https://code.google.com/p/error-prone) you may have issues during the build process from IDEA.  To avoid these issues please install the [Error-prone compiler integration plugin](https://plugins.jetbrains.com/plugin/7349-error-prone-compiler-integration) and build the project using the [`error-prone java compiler`](https://code.google.com/p/error-prone) to catch common Java mistakes at compile-time. To use the compiler, go to _File_ → _Settings_ → _Compiler_ → _Java Compiler_ and select `Javac with error-prone` in the `Use compiler` box.
- Add a custom parameter to avoid a debugging problem.  To use the compiler, go to _File_ → _Settings_ → _Compiler_ → _Java Compiler_ → _Additional command line parameters_ and add `-Xep:ParameterName:OFF`
- Tests: please follow the conventions of the Maven surefire plugin for [unit tests](https://maven.apache.org/surefire/maven-surefire-plugin/examples/inclusion-exclusion.html) and the ones of the Maven failsafe plugin for [integration tests](https://maven.apache.org/surefire/maven-failsafe-plugin/examples/inclusion-exclusion.html). To run the integration tests just run the verify phase, e.g.: `mvn clean verify`

### Understanding branching

We follow a [GitFlow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow) approach to our project.
This can be summarised as:
1. `master` represents what is running in production, or is in the process of being released for a new deployment
2. `gbif-dev` represents the latest state of development, and is what is run by the [continuous build tools](https://builds.gbif.org/)
   4. Generally features branches are made from here 
3. `ala-dev` represents a large feature branch to bring in the `livingatlas` module and pipelines for the ALA work
 
If you are working on a new feature and not part of the core team please ask for guidance on where to start (most likely `gbif-dev`). 

### Make your change

1. Checkout the branch you need (or fork the project and then checkout the branch) 
2. Add unit tests for your change
3. Arrange your commits
    1. Consider merging commits. We favour less commits, but recognise this is not always desirable for large features (please squash small commits addressing typos etc into one) 
4. Use descriptive commit messages that make it easy to identify changes and provide a clear history. 
    1. Please reference the issue you are working on unless it is a trivial change
    2. Examples of good commit messages:
          1. `#123 Enable occurrenceStatus interpretation to avro` 
          1. `Fixup: Addressing typos in JDoc`
    2. Examples of bad commit messages:
          1. ` ` 
          1. `Various fixes`
          1. `#123`
5. Check your code compiles, and all project unit tests pass (please be kind to reviewers)
6. Explore the `errorprone` warnings raised at compilation time. Please address issues you see as best you can.
6. Prefer to create a pull request and have it reviewed for larger submissions. Committers are free to push smaller changes directly.

