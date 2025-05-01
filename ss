// ✅ backend/Jenkinsfile
pipeline {
    agent any

    environment {
        DOCKER_IMAGE = "your-dockerhub-id/backend"
    }

    stages {
        stage('Checkout') {
            steps {
                dir('backend') {
                    git branch: 'main', url: 'https://github.com/your-org/your-repo.git'
                }
            }
        }

        stage('Build Docker Image') {
            steps {
                dir('backend') {
                    sh 'docker build -t $DOCKER_IMAGE:$BUILD_NUMBER .'
                }
            }
        }

        stage('Push to DockerHub') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'docker-hub', usernameVariable: 'DOCKER_USER', passwordVariable: 'DOCKER_PASS')]) {
                    sh 'echo $DOCKER_PASS | docker login -u $DOCKER_USER --password-stdin'
                    sh 'docker push $DOCKER_IMAGE:$BUILD_NUMBER'
                }
            }
        }

        stage('Deploy') {
            steps {
                sshagent(credentials: ['deploy-server']) {
                    sh 'ssh user@deploy-server "docker pull $DOCKER_IMAGE:$BUILD_NUMBER && docker rm -f backend || true && docker run -d --name backend -p 8081:8080 $DOCKER_IMAGE:$BUILD_NUMBER"'
                }
            }
        }
    }
}


// ✅ ml-backend/Jenkinsfile
pipeline {
    agent any

    environment {
        DOCKER_IMAGE = "your-dockerhub-id/ml-backend"
    }

    stages {
        stage('Checkout') {
            steps {
                dir('ml-backend') {
                    git branch: 'main', url: 'https://github.com/your-org/your-repo.git'
                }
            }
        }

        stage('Build Docker Image') {
            steps {
                dir('ml-backend') {
                    sh 'docker build -t $DOCKER_IMAGE:$BUILD_NUMBER .'
                }
            }
        }

        stage('Push to DockerHub') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'docker-hub', usernameVariable: 'DOCKER_USER', passwordVariable: 'DOCKER_PASS')]) {
                    sh 'echo $DOCKER_PASS | docker login -u $DOCKER_USER --password-stdin'
                    sh 'docker push $DOCKER_IMAGE:$BUILD_NUMBER'
                }
            }
        }

        stage('Deploy') {
            steps {
                sshagent(credentials: ['deploy-server']) {
                    sh 'ssh user@deploy-server "docker pull $DOCKER_IMAGE:$BUILD_NUMBER && docker rm -f ml-backend || true && docker run -d --name ml-backend -p 5000:5000 $DOCKER_IMAGE:$BUILD_NUMBER"'
                }
            }
        }
    }
}


// ✅ nginx/Jenkinsfile
pipeline {
    agent any

    environment {
        DOCKER_IMAGE = "your-dockerhub-id/nginx"
    }

    stages {
        stage('Checkout') {
            steps {
                dir('nginx') {
                    git branch: 'main', url: 'https://github.com/your-org/your-repo.git'
                }
            }
        }

        stage('Build Docker Image') {
            steps {
                dir('nginx') {
                    sh 'docker build -t $DOCKER_IMAGE:$BUILD_NUMBER .'
                }
            }
        }

        stage('Push to DockerHub') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'docker-hub', usernameVariable: 'DOCKER_USER', passwordVariable: 'DOCKER_PASS')]) {
                    sh 'echo $DOCKER_PASS | docker login -u $DOCKER_USER --password-stdin'
                    sh 'docker push $DOCKER_IMAGE:$BUILD_NUMBER'
                }
            }
        }

        stage('Deploy') {
            steps {
                sshagent(credentials: ['deploy-server']) {
                    sh 'ssh user@deploy-server "docker pull $DOCKER_IMAGE:$BUILD_NUMBER && docker rm -f nginx || true && docker run -d --name nginx -p 8082:80 $DOCKER_IMAGE:$BUILD_NUMBER"'
                }
            }
        }
    }
}
