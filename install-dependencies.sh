#!/usr/bin/env sh

git clone -b hotfix/0.5 https://github.com/opencb/opencga.git
cd opencga
mvn install -DskipTests
cd ..

git clone https://github.com/EBIvariation/biodata.git
cd biodata
git checkout hotfix/0.4
mvn install -DskipTests
cd ..

