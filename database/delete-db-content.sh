#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

/bin/bash ${SCRIPT_DIR}/connect-to-db.sh << EOF
\i ${SCRIPT_DIR}/delete_db_content.sql;
EOF