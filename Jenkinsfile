pipeline {
  agent any
  parameters {
    choice(
      name: 'TYPE',
      choices: ['QUICK', 'FULL'],
      description: 'Build types:<p>QUICK: Compile, Build, Deploy artifacts, Skip integration tests and extra artifacts, Multithread build<p>FULL: Compile, Build, Deploy artifacts, Run integration tests and extra artifacts, Singlethread build\n'
    )
  }
  tools {
    maven 'Maven 3.8.5'
    jdk 'OpenJDK17'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '10'))
    skipStagesAfterUnstable()
    timestamps()
  }

  stages {

    stage('Quick build') {
      tools {
        jdk 'OpenJDK17'
      }
      when {
        expression {
          params.TYPE == 'QUICK'
        }
      }
      steps {
        sh 'mvn clean verify -DskipITs -U -T 1 -P skip-release-it,pre-backbone-release-artifact'
      }
    }

    stage('Full build') {
      tools {
        jdk 'OpenJDK17'
      }
      when {
        expression {
          params.TYPE == 'FULL'
        }
      }
      steps {
        sh 'mvn clean verify -U -P coverage'
      }
    }

    stage('Snapshots to nexus') {
      environment {
        PROFILES = getProfiles()
      }
      steps {
        configFileProvider([configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')]) {
          sh 'mvn -s $MAVEN_SETTINGS deploy -B -P $PROFILES -DskipTests'
        }
      }
    }

    stage('Build and push Docker images: Ingestion') {
      steps {
        sh 'build/ingestion-docker-build.sh'
      }
    }

    stage('Build and push Docker images: GBIF Impact') {
      steps {
        sh 'build/gbif-impact-docker-build.sh'
      }
    }
  }

    post {
      success {
        echo 'Pipeline executed successfully!'
      }
      failure {
        echo 'Pipeline execution failed!'
    }
  }
}

def getProfiles() {
  def profiles = "skip-release-it,gbif-artifacts,pre-backbone-release-artifact"
  if (params.TYPE == 'FULL') {
      profiles += ",extra-artifacts"
  }
  return profiles
}
