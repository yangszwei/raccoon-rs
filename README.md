# raccoon-rs

A DICOM PACS built in Rust.

## Quick Start

You can run the monolith server with:

```shell
cargo run -p raccoon
```

## Configuration

The monolith binary runs with built-in defaults, so no configuration file is
required. Optional overrides can be provided through local TOML files or
`RACCOON__...` environment variables.

See [config/](./config) for example configuration.

## License

Licensed under the terms of the [LICENSE](LICENSE) file.
