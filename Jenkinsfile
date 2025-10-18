pipeline {
    agent any

    tools {
        jdk 'jdk21'
        maven 'maven3'
    }

    environment {
        SONAR_HOST_URL = 'http://139.59.14.75:9000'
        SONAR_AUTH_TOKEN = credentials('SONAR_AUTH_TOKEN')
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
                echo "🔀 Branch: ${env.BRANCH_NAME}"
            }
        }

        stage('Build') {
            steps {
                sh 'mvn clean install -DskipTests'
            }
        }

        stage('Lint Checks') {
            steps {
                sh '''
                    echo "🧹 Running Lint Analysis..."
                    mvn checkstyle:check spotbugs:spotbugs pmd:check || true
                '''
            }
        }

        stage('Code Coverage') {
            steps {
                sh 'mvn jacoco:report'
            }
        }

        stage('SonarQube Analysis') {
            steps {
                withSonarQubeEnv('SonarQubeServer') {
                    echo "🚀 Running SonarQube Analysis for ${env.BRANCH_NAME}"
                    sh """
                        mvn sonar:sonar \
                          -Dsonar.projectKey=kafkademoapplication \
                          -Dsonar.projectName="Kafka Demo Application" \
                          -Dsonar.host.url=$SONAR_HOST_URL \
                          -Dsonar.login=$SONAR_AUTH_TOKEN \
                          -Dsonar.sources=src/main/java \
                          -Dsonar.tests=src/test/java \
                          -Dsonar.java.binaries=target/classes \
                          -Dsonar.junit.reportPaths=target/surefire-reports \
                          -Dsonar.coverage.jacoco.xmlReportPaths=target/site/jacoco/jacoco.xml \
                          -Dsonar.java.checkstyle.reportPaths=target/checkstyle-result.xml \
                          -Dsonar.java.spotbugs.reportPaths=target/spotbugsXml.xml \
                          -Dsonar.java.pmd.reportPaths=target/pmd.xml
                    """
                }
            }
        }
    }

    post {
        success {
            echo "✅ SonarQube Analysis completed successfully for ${env.BRANCH_NAME}"
        }
        failure {
            echo "❌ Build or analysis failed for ${env.BRANCH_NAME}"
        }
    }
}
