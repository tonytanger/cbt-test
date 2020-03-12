#!/usr/bin/env sh
echo "run directpath cbt test"
GOOGLE_CLOUD_ENABLE_DIRECT_PATH=bigtable ./gradlew run -PmainClass=com.on.DirectPathCbtTest

echo "run normal cbt test"
GOOGLE_CLOUD_ENABLE_DIRECT_PATH= ./gradlew run -PmainClass=com.on.NormalCbtTest
