#!/bin/bash

# Set the paths for your SQL file and expected output
SQL_FILE="das_mock_test.sql"
EXPECTED_OUTPUT="das_mock_test.expected"
ACTUAL_OUTPUT="das_mock_test.actual"

# Run the SQL file and save the output
psql -f "$SQL_FILE" > "$ACTUAL_OUTPUT"

# Compare the actual output to the expected output
if diff -u "$EXPECTED_OUTPUT" "$ACTUAL_OUTPUT"; then
    echo "✅ Test passed: Output matches expected results."
    rm "$ACTUAL_OUTPUT" # Clean up if test passes
else
    echo "❌ Test failed: Differences found."
    echo "Check the differences above and inspect $ACTUAL_OUTPUT for details."
fi
