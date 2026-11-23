#!/bin/bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -euo pipefail

failed=0

# findImports prints every non-vendored Go file line that imports the given path, or a package
# under it when the path ends with a slash.
findImports() {
    grep -rn --include='*.go' --exclude-dir=vendor "\"$1" . || true
}

# Check that packages are not imported from github.com/hyperledger/fabric/
FABRIC_IMPORT="github.com/hyperledger/fabric/"
FABRIC_PROTOUTIL="github.com/hyperledger/fabric/protoutil"

found=$(findImports "$FABRIC_IMPORT")
if [[ -n "$found" ]]; then
    echo "The following files import from $FABRIC_IMPORT:"
    echo "$found"
    echo "Use github.com/Chaguar/common (or fabric-lib-go / fabric-protos-go-apiv2) instead."
    if echo "$found" | grep -q "$FABRIC_PROTOUTIL"; then
        echo "Use github.com/Chaguar/common/protoutil instead."
    fi
    failed=1
fi

# Check that github.com/pkg/errors is not imported. It is archived, and the repository uses the
# standard library instead.
PKG_ERRORS_IMPORT="github.com/pkg/errors\""

found=$(findImports "$PKG_ERRORS_IMPORT")
if [[ -n "$found" ]]; then
    echo "The following files import github.com/pkg/errors:"
    echo "$found"
    echo "Use the standard library instead: errors.New, fmt.Errorf with %w, errors.Is, errors.As."
    failed=1
fi

exit "$failed"
