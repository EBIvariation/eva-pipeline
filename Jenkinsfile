pipeline {
  agent {
    docker {
      image 'maven:3.5.0-jdk-8'
      args '-v /root/.m2:/root/.m2'
    }
    
  }
  stages {
    stage('Initialize') {
      steps {
        echo 'EVA Pipeline :: Initialize'
        sh '''chmod +x install-dependencies.sh
wget http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-$MONGODB_VERSION.tgz
tar xfz mongodb-linux-x86_64-$MONGODB_VERSION.tgz
export PATH=`pwd`/mongodb-linux-x86_64-$MONGODB_VERSION/bin:$PATH
mkdir -p data/db
mongod --dbpath=data/db &
./install-dependencies.sh
mongod --version
mvn clean'''
        sh '''echo PATH = ${PATH}
echo M2_HOME = ${M2_HOME}'''
      }
    }
    stage('Build') {
      steps {
        echo 'EVA Pipeline :: Build'
        sh 'mvn -Dmaven.test.failure.ignore= true install'
      }
    }
    stage('Report') {
      steps {
        echo 'EVA Pipeline :: Report'
        junit 'target/surefire-reports/**/*.xml'
      }
    }
  }
}