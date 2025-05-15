pipeline {
    agent any

    environment {
        IMAGE_NAME = "jaerimw/ml-backend:latest"
        DEPLOY_SERVER = "root@host.docker.internal -p 2222"
    }

    triggers {
        githubPush()
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Check ml-backend Changes') {
            steps {
                script {
                    // 변경된 파일 목록 추출
                    def changes = sh(script: "git diff --name-only HEAD~1 HEAD", returnStdout: true).trim()
                    echo "Changed Files:\n${changes}"

                    // ml-backend 디렉터리 변경 없으면 빌드 중단
                    if (!changes.split('\n').any { it.startsWith('ml-backend/') }) {
                        echo "No changes in ml-backend directory. Skipping build."
                        currentBuild.result = 'SUCCESS'
                        error('No ml-backend changes detected')
                    }
                }
            }
        }

        stage('Build Docker Image') {
            steps {
                dir('ml-backend') {
                    sh 'docker build -t $IMAGE_NAME .'
                }
            }
        }

        stage('Push to Docker Hub') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'dockerhub', usernameVariable: 'DOCKER_USER', passwordVariable: 'DOCKER_PASS')]) {
                    sh '''
                    echo $DOCKER_PASS | docker login -u $DOCKER_USER --password-stdin
                    docker push $IMAGE_NAME
                    '''
                }
            }
        }

        stage('Deploy on test-server') {
            steps {
                sshagent(['jenkins-testkey']) {
                    sh '''
                    ssh -o StrictHostKeyChecking=no $DEPLOY_SERVER '
                      docker pull $IMAGE_NAME &&
                      docker stop ml-backend || true &&
                      docker rm ml-backend || true &&
                      docker run -d --name ml-backend -p 5000:5000 $IMAGE_NAME
                    '
                    '''
                }
            }
        }
    }
}
