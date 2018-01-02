# twitter-realtime
An app to list realtime top tweets 

readonly PROGNAME=$(basename $0)
readonly PROGDIR=$(readlink -m $(dirname $0))
readonly ARGS="$@"
readonly APP_HOME=$(readlink -m $(dirname $0))/../
