#!/usr/bin/env sh

git clone -b hotfix/0.4 https://github.com/EBIvariation/biodata.git
cd biodata
mvn install -DskipTests
cd ..

git clone -b hotfix/0.5 https://github.com/EBIVariation/opencga.git
cd opencga
mvn install -DskipTests
cd ..
