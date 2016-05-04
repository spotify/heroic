#!/bin/bash
TMPFILE=$(mktemp)

cleanup() {
    rm "$TMPFILE"
}

trap cleanup 0

for FILE in $(find . -type f -name "*.java" | grep -v '/src/test/' | grep -v '/target/generated-sources/'); do
    # check if file has license.
    # if not, prepend it
    if ! tools/license_matcher.py "$FILE" >/dev/null;then
        echo "Prepend license to $FILE"
        cat tools/java.header "$FILE" > "$TMPFILE"
        cat "$TMPFILE" > "$FILE"
    fi

done
