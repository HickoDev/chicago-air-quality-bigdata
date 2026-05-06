#!/bin/bash
set -euo pipefail

PROJECT_DIR="/root/chicago-air-mapreduce"
BUILD_DIR="${PROJECT_DIR}/build"
CLASSES_DIR="${BUILD_DIR}/classes"
OUTPUT_JAR="/root/chicago-mapreduce.jar"

cd "${PROJECT_DIR}"
rm -rf "${BUILD_DIR}"
mkdir -p "${CLASSES_DIR}"

javac -cp "$(hadoop classpath --glob)" -d "${CLASSES_DIR}" $(find src/main/java -name "*.java")
jar cvf "${OUTPUT_JAR}" -C "${CLASSES_DIR}" .

echo "Built ${OUTPUT_JAR}"
