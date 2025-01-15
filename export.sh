#!/usr/bin/env bash

###################################
function inf  { >&2 printf "\033[34m[+]\033[0m %b\n" "$@" ; }
function warn { >&2 printf "\033[33m[!]\033[0m %b\n" "$@" ; }
function err  { >&2 printf "\033[31m[!]\033[0m %b\n" "$@" ; }
function err! { err "$@" && exit 1; }

unset EXIT_RES
ON_EXIT=("${ON_EXIT[@]}")

function on_exit_fn {
  EXIT_RES=$?
  for cb in "${ON_EXIT[@]}"; do $cb || true; done
  # read might hang on ctrl-c, this is a hack to finish the script for real
  clear_exit
  exit $EXIT_RES
}

function on_exit {
  ON_EXIT+=("$@")
}


function clear_exit {
  trap - EXIT SIGINT
}

trap on_exit_fn EXIT SIGINT

function barsh {
  [[ $# -lt 2 ]] && return 1
  local val=$1; local bas=$2; local txt=$3; local wid=$4;

  [[ -z $wid ]] && { [[ -z $txt ]] && wid=$bas || wid=${#txt} ; }
  [[ -z $txt ]] && txt="$(printf '%*s' "$wid" '')"
  [[ $wid -gt ${#txt} ]] && txt=$txt$(printf '%*s' $((${#txt} - wid)) '')

  local per=$(( (wid * val) / bas ))
  printf "\033[7m%s\033[27m%s" "${txt:0:$per}" "${txt:$per:$((wid-per))}"
}
###################################

ACTION=${ACTION}

EXP=$(basename $0)
EXP_FROM=${EXP_FROM}
EXP_TO=${EXP_TO}
EXP_ALL=${EXP_ALL}
EXP_YEARLY=${EXP_YEARLY:-0}
EXP_MONTHLY=${EXP_MONTHLY:-0}
EXP_NETWORK=${EXP_NETWORK}
EXP_OUT=${EXP_OUT:-'/dev/stdout'}
# anything between 10 and 15 has a good CPU time / filesize ratio
EXP_ZSTD_C_LVL=${EXP_ZSTD_C_LVL:-10}
EXP_FORCE=${EXP_FORCE}

function usage {
  cat << EOF

                              .     '     ,
                                _________
                             _ /_|_____|_\ _
                               '. \   / .'
                                 '.\ /.'
                                   '.'

           $EXP: your bike share import and export specialist

Usage: $EXP action [options...]

Options:
  --from            start date interval (YYYY-MM-DD)
  --to              end date interval (YYYY-MM-DD)
  --network         filter by network tag
  --all             export each network on a separate file
  --monthly         do montly exports on interval
  --yearly          do yearly exports on interval
  --zstd            ZSTD compression level
  -f, --force       force action
  -o, --out         export output file (defaults to stdout)
  -V, --verbose     echo every command that gets executed
  -h, --help        display this help

Commands:
  help                            Show usage

  quack cb.db cb.duck             SQLite DB to duckdb

  parquet cb.duck                 Export stat data as parquet file
  csv cb.duck                     Export stat data as csv file
  custom cb.duck                  Use custom format (duckdb)

  Example:
    $ $EXP quack cb.db cb.duck
    $ $EXP parquet cb.duck --from 2024-11-01 --to 2024-11-05 > out.parquet
    $ $EXP csv cb.duck --from 2024-11-01 --to 2024-11-05 -o out.csv
    $ $EXP parquet --all --from 2024-11-01 --to 2024-12-01 -o dump/202411
    $ $EXP custom cb.duck --network bicing -o out.parquet -- \\
        "FORMAT 'parquet', CODEC 'zstd', COMPRESSION_LEVEL 22"

EOF
}


function parse_args {
  _ARGS=()
  _NP_ARGS=()

  ! [[ $1 =~ ^- ]] && ACTION=$1 && shift
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -V|--verbose)
        set -x
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      --from)
        EXP_FROM=$2
        shift
        ;;
      --to)
        EXP_TO=$2
        shift
        ;;
      --network)
        EXP_NETWORK=$2
        shift
        ;;
      --yearly)
        EXP_YEARLY=1
        ;;
      --monthly)
        EXP_MONTHLY=1
        ;;
      --zstd)
        EXP_ZSTD_C_LVL=$2
        shift
        ;;
      --all)
        EXP_ALL=1
        ;;
      -o|--out)
        EXP_OUT=$2
        shift
        ;;
      -f|--force)
        EXP_FORCE=1
        ;;
      -)
        _ARGS+=("$(cat "$2")")
        shift
        ;;
      --)
        shift
        _NP_ARGS+=("$@")
        break
        ;;
      *)
        _ARGS+=("$1")
        ;;
    esac
    shift
  done
}


function export_all {
  local networks=($(duckdb $1 -readonly --list --noheader << EOF
  SELECT DISTINCT(network_tag) FROM stats
  WHERE true
    ${EXP_FROM:+"AND timestamp > '$EXP_FROM'"}
    ${EXP_TO:+"AND timestamp < '$EXP_TO'"}
  ORDER BY network_tag ASC
  ;
EOF
))
  [[ $EXP_OUT == "/dev/stdout" ]] && err! "Output can't be stdout"
  local where=$(dirname $EXP_OUT)
  mkdir -p $where

  local bname=$(basename $EXP_OUT)
  local prefix=${bname%.*}
  local extension=${bname#*.}
  [[ "$prefix" == "$extension" ]] && extension="$2"

  local args=()
  args+=("$2")
  args+=("$1")
  [[ -n $EXP_FROM ]] && args+=("--from $EXP_FROM")
  [[ -n $EXP_TO ]] && args+=("--to $EXP_TO")

  local filename
  local i=1
  local per

  for network in ${networks[@]}; do
    per=$(( ($i * 100) / ${#networks[@]} ))
    filename="$where/$prefix-$network-stats.$extension"
    [[ -f $filename ]] && [[ -z $EXP_FORCE ]] && \
      err! "File exists, use -f to force overwrite"
    printf "\033[0G\033[K\033[34m%b\033[0m" \
      "$(barsh $i ${#networks[@]} \
        "│$i/${#networks[@]}:${per}%│ $network → $filename" $COLUMNS)"
    # XXX kind of a hack
    if [[ $2 == "csv.gz" ]]; then
      $(realpath $0) ${args[@]} --network $network | gzip -cf > $filename
    else
      $(realpath $0) ${args[@]} --network $network -o $filename &> /dev/null
    fi
    ((i+=1))
  done
}


function main {
  parse_args "$@"

  # re-set action arguments after parsing.
  # access action arguments as $1, $2, ... in order
  set -- "${_ARGS[@]}"

  case $ACTION in
      # Dumps sqlite db into a CSV with json columns and imports them on
      # a duckdb. Necessary because duckdb does not support JSONB columns
      quack|duck)
        [[ -z $1 ]] && err! "Please provide input sqlite db filename"
        local sqlfile=$(realpath $1); shift
        ! [[ -f $sqlfile ]] && err! "$sqlfile not found"
        local duckfile=${1:-${sqlfile%.*}.duck}
        [[ -f $duckfile ]] && [[ -z $EXP_FORCE ]] && \
          err! "$duckfile exists, use -f to force" || \
          warn "quacking $duckfile"
        tmpfile=$(mktemp -u cb-export.csv.XXXXX)
        inf "Converting to CSV in $tmpfile"
        on_exit "rm -f $tmpfile"

        sqlite3 --csv $sqlfile << EOF > $tmpfile
SELECT network_tag, json(station), timestamp FROM stats
WHERE true
  ${EXP_FROM:+"AND timestamp > '$EXP_FROM'"}
  ${EXP_TO:+"AND timestamp < '$EXP_TO'"}
  ${EXP_NETWORK:+"AND network_tag = '$EXP_NETWORK'"}
;
EOF
        inf "Populating $duckfile with data"
        duckdb $duckfile << EOF
CREATE TABLE IF NOT EXISTS stats (
    network_tag VARCHAR,
    station JSON,
    timestamp TIMESTAMP
);

COPY stats from '$tmpfile';

-- useful view for manual working with duck db file
CREATE VIEW IF NOT EXISTS _deduped AS (
    WITH st_window AS (
        SELECT network_tag,
               station,
               station.bikes AS bikes,
               station.free AS free,
               lag(station.bikes, 1, NULL) OVER st AS prev_bikes,
               lag(station.free,  1, NULL) OVER st AS prev_free,
               timestamp
        FROM stats
        WINDOW st AS(
            PARTITION BY network_tag, station.id, station.extra.uid
            ORDER BY timestamp ASC
        )
        ORDER BY network_tag, station.id, station.extra.uid,
                 timestamp ASC
    )
    SELECT * FROM st_window
    WHERE
        bikes <> prev_bikes OR free <> prev_free OR
        (prev_bikes IS NULL AND prev_free IS NULL)
);
EOF
        ;;
      parquet|csv|csv.gz|custom)

        ! [[ -f $1 ]] && err! "Please provide a duckdb"

        local duckfile=$1; shift

        [[ -n $EXP_ALL ]] && {
          export_all $duckfile $ACTION
          exit $?
        }

        local format

        case $ACTION in
            parquet)
              format="FORMAT 'PARQUET', \
                      CODEC 'ZSTD', \
                      COMPRESSION_LEVEL $EXP_ZSTD_C_LVL"
              ;;
            csv)
              format="FORMAT 'csv', HEADER"
              ;;
            csv.gz)
              format="FORMAT 'csv', HEADER"
              ;;
            custom)
              format="${_NP_ARGS[@]}"
              ;;
            *)
              err! "Unrecognized format $ACTION"
              ;;
        esac

        duckdb -readonly $duckfile << EOF
COPY (
    WITH st_window AS (
        SELECT network_tag,
               station,
               station.bikes AS bikes,
               station.free AS free,
               lag(station.bikes, 1, NULL) OVER st AS prev_bikes,
               lag(station.free,  1, NULL) OVER st AS prev_free,
               timestamp
        FROM stats
        WHERE true
          ${EXP_FROM:+"AND timestamp > '$EXP_FROM'"}
          ${EXP_TO:+"AND timestamp < '$EXP_TO'"}
          ${EXP_NETWORK:+"AND network_tag = '$EXP_NETWORK'"}
        WINDOW st AS(
            PARTITION BY network_tag, station.id, station.extra.uid
            ORDER BY timestamp ASC
        )
        ORDER BY network_tag, station.id, station.extra.uid,
                 timestamp ASC
    ),
    deduped AS (
      SELECT * FROM st_window
      WHERE
          bikes <> prev_bikes OR free <> prev_free OR
          (prev_bikes IS NULL and prev_free IS NULL)
    )

    SELECT network_tag AS tag,
           json_extract_string(station, '$.id') AS id,
           json_extract_string(station, '$.extra.uid') AS nuid,
           json_extract_string(station, '$.name') AS name,
           json_extract(station, '$.latitude')::double AS latitude,
           json_extract(station, '$.longitude')::double AS longitude,
           json_extract(station, '$.bikes')::int AS bikes,
           json_extract(station, '$.free')::int AS free,
           station.extra,
           timestamp
    FROM deduped
    WHERE true
      ${EXP_FROM:+"AND timestamp > '$EXP_FROM'"}
      ${EXP_TO:+"AND timestamp < '$EXP_TO'"}
      ${EXP_NETWORK:+"AND network_tag = '$EXP_NETWORK'"}
    ORDER BY tag, id, nuid, timestamp ASC
) TO '$EXP_OUT' WITH ($format);
EOF
        ;;
      help)
        usage
        ;;
      *)
        usage
        exit 1
        ;;
  esac
}

main "$@"
