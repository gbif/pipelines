pipeline {
  agent any
  parameters {
    choice(
      name: 'TYPE',
      choices: ['QUICK', 'FULL'],
      description: 'Build types:<p>QUICK: Compile, Build, Deploy artifacts, Skip integration tests and extra artifacts, Multithread build<p>FULL: Compile, Build, Deploy artifacts, Run integration tests and extra artifacts, Singlethread build\n'
    )
    booleanParam(name: 'RELEASE', defaultValue: false, description: 'Make a Maven release')
  }
  tools {
    maven 'Maven 3.8.5'
    jdk 'OpenJDK11'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '10'))
    skipStagesAfterUnstable()
    timestamps()
    disableConcurrentBuilds()
  }
  stages {
    stage('Validate') {
      when {
        allOf {
          expression { params.RELEASE }
          not {
             branch 'master'
          }
        }
      }
      steps {
        script {
          error('Releases are only allowed from the master branch.')
        }
      }
    }
    stage('Setup') {
      steps {
        script {
          env.VERSION = """${sh(returnStdout: true, script: './build/get-version.sh ${RELEASE}')}"""
          if (params.RELEASE) {
            env.BUILD_TYPE = 'FULL'
          } else {
            env.BUILD_TYPE = params.TYPE
          }
        }
      }
    }
    stage('Quick build') {
      when {
        expression {
          env.BUILD_TYPE == 'QUICK'
        }
      }
      steps {
        sh 'mvn clean verify -U -T 3 -P skip-coverage,skip-release-it'
      }
    }
    stage('Full build') {
      when {
        expression {
          env.BUILD_TYPE == 'FULL'
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
          sh 'mvn -s $MAVEN_SETTINGS deploy -B -P ${PROFILES} -DskipTests'
        }
      }
    }
    stage('Release version to nexus') {
      environment {
        PROFILES = getProfiles()
      }
      when {
        allOf {
          expression { params.RELEASE }
          branch 'master'
        }
      }
      steps {
        configFileProvider([configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')]) {
          git 'https://github.com/gbif/pipelines.git'
          sh 'mvn -s $MAVEN_SETTINGS release:prepare release:perform -Denforcer.skip=true -DskipTests -P ${PROFILES}'
        }
      }
    }
    stage('Build and publish Docker image') {
      steps {
        sh 'build/ingestion-docker-build.sh ${RELEASE} ${VERSION}'
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
  def profiles = "skip-coverage,skip-release-it,gbif-artifacts"
  if (env.BUILD_TYPE == 'FULL') {
      profiles += ",extra-artifacts"
  }
  return profiles
}
