pipeline {
  agent any
  tools {
    maven 'Maven3.6'
    jdk 'JDK8'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '10'))
    timestamps ()
  }
  parameters {
    booleanParam(name: 'DOCUMENTATION',
            defaultValue: false,
            description: 'Generate API documentation')
    separator(name: "release_separator", sectionHeader: "Release Parameters")
    booleanParam(name: 'RELEASE',
            defaultValue: false,
            description: 'Do a Maven release (it also generates API documentation)')
    string(name: 'RELEASE_VERSION', defaultValue: '', description: 'Release version (optional)')
    string(name: 'DEVELOPMENT_VERSION', defaultValue: '', description: 'Development version (optional)')
    booleanParam(name: 'DRY_RUN_RELEASE', defaultValue: false, description: 'Dry Run Maven release')
  }
  stages {
    stage('Preconditions') {
      steps {
        scmSkip(skipPattern:'.*(\\[maven-release-plugin\\] prepare release |Generated API documentation|Google Java Format).*')
      }
    }
    stage('Build') {
      when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.DOCUMENTATION } };
        }
      }
      steps {
        withMaven {
          sh 'mvn clean package install -T 2C -e -DskipTests -DskipITs -Ddocker.skip.run'
        }
      }
    }
    stage('Tests') {
      when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.DOCUMENTATION } };
        }
      }
      failFast true
      parallel {
        stage('Unit tests') {
          steps {
            configFileProvider([configFile(
                    fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                    variable: 'MAVEN_SETTINGS_XML')]) {
              sh 'mvn surefire:test -T 2C -Dparallel=classes -DuseUnlimitedThreads=true -e -Pcoverage -Ddocker.skip.run -DskipITs'
            }
          }
        }
        stage('Integration tests') {
          environment {
            ALANM_PORT = findFreePort()
            ALANM_ADMIN_PORT = findFreePort()
            ALA_SOLR_PORT = findFreePort()
            ALA_ZK_PORT = "${sh(script:'$(($ALA_SOLR_PORT+1000))', returnStdout: true)}"
            SDS_ADMIN_PORT = findFreePort()
            SDS_PORT = findFreePort()
          }
          steps {
            configFileProvider([configFile(
                    fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                    variable: 'MAVEN_SETTINGS_XML')]) {
              sh 'mvn resources:testResources docker:build docker:start failsafe:integration-test docker:stop -T 1C -Dparallel=classes -DuseUnlimitedThreads=true -e -Pcoverage -Dalanm.port=$ALANM_PORT -Dalanm.admin.port=$ALANM_ADMIN_PORT -Dsolr8.zk.port=$ALA_ZK_PORT -Dsolr8.http.port=$ALA_SOLR_PORT -Dsds.admin.port=$SDS_ADMIN_PORT -Dsds.port=$SDS_PORT'
            }
          }
        }
      }
    }
    stage('SonarQube analysis') {
      when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.DOCUMENTATION } };
        }
      }
      steps {
        withSonarQubeEnv('GBIF Sonarqube') {
          sh 'mvn sonar:sonar'
        }
      }
    }
    stage('Snapshot to nexus') {
      when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.DOCUMENTATION } };
          branch 'master';
        }
      }
      steps {
        configFileProvider(
                [configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                        variable: 'MAVEN_SETTINGS_XML')]) {
          sh 'mvn -s $MAVEN_SETTINGS_XML -B -DskipTests deploy'
        }
      }
    }
    stage('Release version to nexus') {
      when {
        allOf {
          expression { params.RELEASE };
          branch 'master';
        }
      }
      environment {
        RELEASE_ARGS = createReleaseArgs()
      }
      steps {
        configFileProvider(
                [configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                        variable: 'MAVEN_SETTINGS_XML')]) {
          git 'https://github.com/gbif/vocabulary.git'
          sh 'mvn -s $MAVEN_SETTINGS_XML -B release:prepare release:perform $RELEASE_ARGS'
        }
      }
    }
  }
  post {
    failure {
      slackSend message: "Pipelines build failed! - ${env.JOB_NAME} ${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>)",
              channel: "#dev"
    }
  }
}


int findFreePort(){
  return new ServerSocket(0).withCloseable { socket -> socket.getLocalPort() }
}