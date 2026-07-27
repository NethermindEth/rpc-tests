#!/usr/bin/env bash
set -euo pipefail

keep=false
args=()
for arg in "$@"; do
    if [[ "$arg" == "--keep" ]]; then keep=true; else args+=("$arg"); fi
done

folder="${args[0]:?Usage: $0 [--keep] <folder> [min-size]  (e.g. $0 ./logs 1MB)}"
min_size="${args[1]:-0}"

# numfmt accepts K/M/G but not KB/MB/GB — strip trailing B after a unit letter
normalized=$(echo "$min_size" | tr '[:lower:]' '[:upper:]' | sed 's/\([KMGT]\)B$/\1/')

min_bytes=$(numfmt --from=iec "$normalized") || {
    echo "Error: invalid size '$min_size'. Use formats like 1K, 100K, 1M, 1G (or 1KB, 100KB, 1MB)" >&2
    exit 1
}

find "$folder" -maxdepth 1 -type f -name "*.json" -print0 | while IFS= read -r -d '' file; do
    file_size=$(stat -c%s "$file")
    if (( file_size >= min_bytes )); then
        tar -cjf "${file%.*}.tar" -C "$(dirname "$(realpath "$file")")" "$(basename "$file")"
        $keep || rm "$file"
        echo "Compressed: $(basename "$file")"
    fi
done
