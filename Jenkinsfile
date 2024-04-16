pipeline {
  agent any
  parameters {
    choice(name: 'TYPE', choices: ['DEV', 'NIGHTLY'], description: 'Build type')
  }
  tools {
    maven 'Maven3.6'
    jdk 'OpenJDK11'
  }
  options {
    skipStagesAfterUnstable()
  }

  stages {

    stage('DEV build') {
      when {
        expression {
          params.TYPE == 'DEV'
        }
      }
      steps {
          sh 'mvn clean install verify -U -T 3 -P skip-coverage,skip-release-it,gbif-artifacts'
      }
    }

    stage('Nightly build') {
      when {
        expression {
          params.TYPE == 'NIGHTLY'
        }
      }
      steps {
        sh 'mvn clean install verify -U -P coverage,gbif-artifacts,extra-artifacts'
      }
    }

    stage('Build and push Docker image') {
      when {
        expression {
          params.TYPE == 'NIGHTLY'
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
