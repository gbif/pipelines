pipeline {
  agent any
  parameters {
    choice(name: 'TYPE', choices: ['DEV build', 'Nightly build'], description: 'Type')
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
          params.TYPE == 'DEV build'
        }
      }
      steps {
          sh 'mvn clean install verify -U -T 3 -P skip-coverage,skip-release-it,gbif-artifacts'
      }
    }

    stage('Nightly build') {
      when {
        expression {
          params.TYPE == 'Nightly build'
        }
      }
      steps {
        sh 'mvn clean install verify -U -P coverage,gbif-artifacts,extra-artifacts'
      }
    }

    stage('Build and push Docker image') {
      when {
        expression {
          params.TYPE == 'Nightly build'
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
