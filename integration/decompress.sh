#!/usr/bin/env bash
set -euo pipefail

keep=false
args=()
for arg in "$@"; do
    if [[ "$arg" == "--keep" ]]; then keep=true; else args+=("$arg"); fi
done

folder="${args[0]:?Usage: $0 [--keep] <folder>}"

find "$folder" -maxdepth 1 -type f -name "*.tar" -print0 | while IFS= read -r -d '' file; do
    base=$(basename "$file" .tar)
    inner=$(tar -tf "$file" | head -1)
    tar -xf "$file" -C "$folder"
    [[ "$folder/$inner" != "$folder/${base}.json" ]] && mv "$folder/$inner" "$folder/${base}.json"
    $keep || rm "$file"
    echo "Extracted: ${base}.json"
done
