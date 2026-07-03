#!/usr/bin/env bash
set -euo pipefail

image_repository="${IMAGE_REPOSITORY:-siweiyang/raccoon}"
platform="${PLATFORM:-linux/amd64}"
context_dir="${CONTEXT_DIR:-.}"

images=(
  "application-entity-registry:raccoon-application-entity-registry:runtime"
  "dicomweb-gateway:raccoon-dicomweb-gateway:runtime-dcmtk"
  "dimse-gateway:raccoon-dimse-gateway:runtime"
  "ingest:raccoon-ingest:runtime"
  "query:raccoon-query:runtime"
  "retrieve:raccoon-retrieve:runtime"
  "sync:raccoon-sync:runtime"
)

echo "Pushing images to ${image_repository} for ${platform}"

for image in "${images[@]}"; do
  IFS=: read -r tag package target <<<"${image}"
  full_tag="${image_repository}:${tag}"

  echo
  echo "Building and pushing ${full_tag}"

  docker buildx build \
    --platform "${platform}" \
    --target "${target}" \
    --tag "${full_tag}" \
    --build-arg "PACKAGE=${package}" \
    --push \
    "${context_dir}"
done
