#!/usr/bin/env sh

git clone -b hotfix/0.5 https://github.com/opencb/opencga.git
cd opencga
mvn install -DskipTests

