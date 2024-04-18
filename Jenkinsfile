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
    maven 'Maven3.6'
    jdk 'OpenJDK11'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '10'))
    skipStagesAfterUnstable()
  }

  stages {

    stage('Quick build') {
      when {
        expression {
          params.TYPE == 'QUICK'
        }
      }
      steps {
          sh 'mvn clean verify install -U -T 3 -P skip-coverage,skip-release-it,gbif-artifacts'
      }
    }

    stage('Full build') {
      when {
        expression {
          params.TYPE == 'FULL'
        }
      }
      steps {
        sh 'mvn clean verify install -U -P coverage,gbif-artifacts,extra-artifacts'
      }
    }

    stage('Deploy artifacts') {
      steps {
        configFileProvider([configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709', variable: 'MAVEN_SETTINGS')]) {
          sh 'mvn -s $MAVEN_SETTINGS -B jar:jar deploy:deploy'
        }
      }
    }

    stage('Build and push Docker images') {
      when {
        expression {
          params.TYPE == 'FULL'
        }
      }
      steps {
          sh 'build/clustering-docker-build.sh'
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
