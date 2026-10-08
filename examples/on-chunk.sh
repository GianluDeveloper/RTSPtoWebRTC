#!/bin/sh
# Copy this file to recording-hooks/on-chunk.sh on the host and chmod +x it.
# It executes inside the service container. Install any extra tools in that
# image, and replace the example metadata sidecar operation below as needed.
# Argument 1 is the finalized MP4 path inside the container. Standard input is
# one JSON object with source host/port, stream ID, timing, and chunk details.
set -eu

chunk=${1:?Missing completed MP4 path}
case "$chunk" in
  /*.mp4) ;;
  *) exit 64 ;;
esac

metadata_tmp=$(mktemp "${chunk}.metadata.XXXXXX")
trap 'rm -f "$metadata_tmp"' EXIT HUP INT TERM
cat > "$metadata_tmp"
mv "$metadata_tmp" "${chunk}.metadata.json"

# Add your processing here. Always quote "$chunk" when passing its path.
