#!/bin/sh
# Entry point of the slim image: make sure the configured model files are there, then run the command.
# Only the first start with an empty /models needs the network.
set -eu
sh /opt/tinyasr/scripts/fetch-models.sh "$(dirname "$TINYASR_MODEL")" \
  "$(basename "$TINYASR_MODEL")" "$(basename "$TINYASR_VAD")" ${TINYASR_LID:+"$(basename "$TINYASR_LID")"}
exec "$@"
