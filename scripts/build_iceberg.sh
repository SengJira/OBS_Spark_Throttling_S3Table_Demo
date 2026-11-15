#!/usr/bin/env bash
# Build iceberg-spark-runtime-4.2_2.13-1.12.0.jar from the Apache Iceberg
# 1.12.0 source release (the 4.2 runtime is in the source tree but was not
# published to Maven Central).
#
# Patch applied: RESTSigV4AuthSession sets Aws4SignerParams.doubleUrlEncode(true)
# when rest.signing-name=s3tables. ObjectScale's tablesvc verifies SigV4 using
# S3-style double URL encoding of the canonical path (verified empirically:
# GET /iceberg/v1/<arn>/namespaces only validates when the canonical request
# re-encodes the percent signs). Upstream Iceberg signs with single encoding.
set -euo pipefail
SRC=/opt/iceberg-build
TARBALL=/tmp/apache-iceberg-1.12.0.tar.gz
JAR=spark/v4.2/spark-runtime/build/libs/iceberg-spark-runtime-4.2_2.13-1.12.0.jar

if [ ! -d "$SRC" ]; then
  curl -fSL -o "$TARBALL" \
    https://archive.apache.org/dist/iceberg/apache-iceberg-1.12.0/apache-iceberg-1.12.0.tar.gz
  mkdir -p "$SRC"
  tar xzf "$TARBALL" -C "$SRC" --strip-components=1
fi

PATCH=aws/src/main/java/org/apache/iceberg/aws/RESTSigV4AuthSession.java
if ! grep -q 'doubleUrlEncode' "$SRC/$PATCH"; then
  python3 - "$SRC/$PATCH" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
s = s.replace(
    "            .signingRegion(signingRegion)\n"
    "            .awsCredentials",
    "            .signingRegion(signingRegion)\n"
    "            .doubleUrlEncode(\"s3tables\".equalsIgnoreCase(signingName))\n"
    "            .awsCredentials")
open(p, "w").write(s)
print("patched", p)
PY
fi

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
cd "$SRC"
./gradlew :iceberg-spark:iceberg-spark-runtime-4.2_2.13:shadowJar -x test --no-daemon
cp "$JAR" /opt/spark/jars/
echo "installed /opt/spark/jars/iceberg-spark-runtime-4.2_2.13-1.12.0.jar"
