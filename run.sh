#!/usr/bin/env sh
echo "run directpath cbt test"
./gradlew run -PmainClass=com.on.DirectPathCbtTest

echo "run normal cbt test"
./gradlew run -PmainClass=com.on.NormalCbtTest
