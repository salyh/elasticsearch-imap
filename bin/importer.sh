#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
lib=${DIR}/../lib
java -cp "$lib/*" de.saly.elasticsearch.importer.imap.IMAPImporterCl "$@"