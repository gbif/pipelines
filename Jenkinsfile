pipeline {

  agent any

  tools {
    maven 'Maven 3.8.5'
    jdk 'OpenJDK 11'
  }
  options {
    skipStagesAfterUnstable()
  }

  stages {
    stage('Project DEV build') {
      steps {
        sh 'mvn clean install verify -U -T 3 -P skip-coverage,skip-release-it,gbif-artifacts'
      }
    }

    stage('Build and push Docker image') {
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
