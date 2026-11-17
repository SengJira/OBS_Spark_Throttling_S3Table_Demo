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
#
# NOTE: the resulting jar is ObjectScale-specific. It changes how every
# `rest.signing-name=s3tables` request is signed, so do NOT reuse it against
# AWS S3 Tables (or any other SigV4 REST catalog) without retesting there.
set -euo pipefail
VERSION=1.12.0
SRC=${ICEBERG_SRC:-/opt/iceberg-build}
SPARK_JARS=${SPARK_JARS:-/opt/spark/jars}
BASE_URL=https://archive.apache.org/dist/iceberg/apache-iceberg-$VERSION
TARBALL=/tmp/apache-iceberg-$VERSION.tar.gz
JAR_NAME=iceberg-spark-runtime-4.2_2.13-$VERSION.jar
JAR=spark/v4.2/spark-runtime/build/libs/$JAR_NAME
PATCH=aws/src/main/java/org/apache/iceberg/aws/RESTSigV4AuthSession.java
CLASS=org/apache/iceberg/aws/RESTSigV4AuthSession.class

if [ ! -d "$SRC" ]; then
  curl -fSL -o "$TARBALL" "$BASE_URL/apache-iceberg-$VERSION.tar.gz"
  curl -fSL -o "$TARBALL.sha512" "$BASE_URL/apache-iceberg-$VERSION.tar.gz.sha512"
  # the published .sha512 is "<hex>  <file>" or just "<hex>"; compare the hash only
  want=$(tr -d '\n' < "$TARBALL.sha512" | awk '{print $1}')
  got=$(sha512sum "$TARBALL" | awk '{print $1}')
  if [ -z "$want" ] || [ "$want" != "$got" ]; then
    echo "ERROR: sha512 mismatch for $TARBALL (expected ${want:-<empty>}, got $got)" >&2
    exit 1
  fi
  echo "sha512 verified: $TARBALL"
  mkdir -p "$SRC"
  tar xzf "$TARBALL" -C "$SRC" --strip-components=1
fi

if ! grep -q 'doubleUrlEncode' "$SRC/$PATCH"; then
  python3 - "$SRC/$PATCH" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
old = ("            .signingRegion(signingRegion)\n"
       "            .awsCredentials")
new = ("            .signingRegion(signingRegion)\n"
       "            .doubleUrlEncode(\"s3tables\".equalsIgnoreCase(signingName))\n"
       "            .awsCredentials")
if s.count(old) != 1:
    sys.exit(f"ERROR: patch anchor found {s.count(old)} times in {p} "
             f"(expected exactly 1) — source changed, refusing to build an "
             f"unpatched jar")
open(p, "w").write(s.replace(old, new))
print("patched", p)
PY
fi
grep -q 'doubleUrlEncode("s3tables".equalsIgnoreCase(signingName))' "$SRC/$PATCH" || {
  echo "ERROR: $PATCH is not patched" >&2; exit 1; }

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
cd "$SRC"
./gradlew :iceberg-spark:iceberg-spark-runtime-4.2_2.13:shadowJar -x test --no-daemon

# the built jar must really contain the patched class
verify_jar() {
  local jar=$1 tmp
  tmp=$(mktemp -d)
  (cd "$tmp" && unzip -q -o "$jar" "$CLASS")
  if ! "$JAVA_HOME/bin/javap" -c -p -cp "$tmp" org.apache.iceberg.aws.RESTSigV4AuthSession \
       | grep -q 'doubleUrlEncode'; then
    rm -rf "$tmp"
    echo "ERROR: $jar does not contain the doubleUrlEncode patch" >&2
    return 1
  fi
  rm -rf "$tmp"
  echo "verified patch in $jar"
}
verify_jar "$SRC/$JAR"
cp "$JAR" "$SPARK_JARS/"
verify_jar "$SPARK_JARS/$JAR_NAME"
echo "installed $SPARK_JARS/$JAR_NAME"
