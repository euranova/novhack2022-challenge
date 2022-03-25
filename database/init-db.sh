#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

/bin/bash ${SCRIPT_DIR}/connect-to-db.sh << EOF
\i ${SCRIPT_DIR}/init_db.sql;
EOF