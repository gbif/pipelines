pipeline {
  agent any
  parameters {
    choice(
      name: 'TYPE',
      choices: ['QUICK', 'FULL'],
      description: 'Build types:<p>QUICK: Compile, Build, Deploy artifacts, Skip integration tests and extra artifacts, Multithread build<p>FULL: Compile, Build, Deploy artifacts, Run integration tests and extra artifacts, Singlethread build\n'
    )
    booleanParam(name: 'RELEASE', defaultValue: false, description: 'Make a Maven release')
    booleanParam(name: 'DRY_RUN', defaultValue: false, description: 'Test run before release')
  }
  tools {
    maven 'Maven 3.9.9'
    jdk 'OpenJDK17'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '10'))
    skipStagesAfterUnstable()
    timestamps()
    disableConcurrentBuilds()
  }
  triggers {
    snapshotDependencies()
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
      tools {
        jdk 'OpenJDK17'
      }
      when {
        expression {
          env.BUILD_TYPE == 'QUICK'
        }
      }
      steps {
        withMaven () {
          sh 'mvn clean verify -U -T 3 -P skip-release-it,pre-backbone-release-artifact'
        }
      }
    }

    stage('Full build') {
      tools {
        jdk 'OpenJDK17'
      }
      when {
        expression {
          env.BUILD_TYPE == 'FULL' && env.DRY_RUN == 'false'
        }
      }
      steps {
        sh 'mvn clean verify -U'
      }
    }
    stage('Snapshots to nexus') {
      environment {
        PROFILES = getProfiles()
      }
      when {
        expression {
          env.RELEASE == 'false'
        }
      }
      steps {
        configFileProvider([configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')]) {
          sh 'mvn -s $MAVEN_SETTINGS deploy -B -DskipTests -P ${PROFILES}'
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
          sh 'mvn -s $MAVEN_SETTINGS -B release:prepare release:perform -Denforcer.skip=true -Dmaven.test.skip=true -P ${PROFILES}'
        }
      }
    }
    stage('Build and publish Docker image') {
      when {
        expression {
          env.DRY_RUN == 'false'
        }
      }
      steps {
        sh 'build/ingestion-docker-build.sh ${RELEASE} ${VERSION}'
      }
    }

    stage('Build and push Docker images: GBIF Impact') {
      steps {
        sh 'build/gbif-impact-docker-build.sh ${RELEASE} ${VERSION}'
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
    cleanup {
      deleteDir()
    }
  }
}

def getProfiles() {
  def profiles = "skip-release-it,gbif-artifacts,pre-backbone-release-artifact"
  if (env.BUILD_TYPE == 'FULL') {
      profiles += ",extra-artifacts"
  }
  if (env.DRY_RUN == 'true') {
      profiles += " -DdryRun"
  }
  return profiles
}
