# elasticsearch-delete-by-query

_Quick and dirty utility to launch and monitor delete by query on an Elasticsearch cluster_

Relauch the delete by query if it fails, waiting for a completion without failures.

## Installation

You need a working Rust toolchain to install this.

From a terminal, run:

```
cargo install --git https://github.com/zenria/elasticsearch-delete-by-query.git
```

## Usage

```
elasticsearch-delete-by-query 0.1.0

USAGE:
    elasticsearch-delete-by-query [OPTIONS] <query>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -i, --index <index>                                  [default: *]
    -p, --pause-on-errors <pause-on-errors-secs>
            Number of seconds to wait if an error occurs before retring to delete by query [default: 300]

    -r, --requests-per-seconds <requests-per-second>     [default: 100]
    -s, --scroll-size <scroll-size>                     Scroll size parameter (batch size)
    -u, --url <url>                                      [default: http://localhost:9200]

ARGS:
    <query>    JSON encoded query eg: {"range":{"lastIndexingDate":{"lte":"now-3y"}}}```

Cancel the running task upon exit (handle properly termination signals): you can 
press Ctrl-C without lefting a long running task behind...
```

## Disclaimer

It has only been tested against a 6.8 cluster.

## License

MIT
