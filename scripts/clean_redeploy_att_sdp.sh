#!/usr/bin/env bash
# Alias — full wipe + redeploy + demo flow
exec "$(dirname "$0")/deploy_from_scratch.sh" "$@"
