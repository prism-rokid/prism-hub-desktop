# Prism Hub

Prism Hub connects supported desktop agents to Prism clients. It runs locally as a sidecar managed by Prism Desktop and communicates with agent-specific plugins over the PluginBridge protocol.

## What it does

- Manages local plugin processes and their lifecycle.
- Routes Prism Realtime commands to plugins running on the same computer.
- Publishes plugin capabilities and lightweight session state.
- Provides the local control surface embedded in Prism Desktop.

The Hub does not include the Prism cloud services or the official agent plugins. A Prism account and access to the Prism production services are required for remote use. Plugins are separate projects; see the official [plugin repositories](https://github.com/orgs/Prism-Rokid/repositories?q=prism-plugin).

## Requirements

- Go 1.22 or later
- Node.js 22 and Rust toolchain to build Prism Desktop
- A supported desktop agent and its corresponding plugin for agent integration
- A Prism account for cloud-connected features

## Build

Build the Hub sidecar:

```sh
go build -o ./bin/prism-sidecar ./cmd/prism
```

Build the Prism Desktop app from `desktop-app/`:

```sh
cd desktop-app
npm ci
PRISM_SKIP_CODESIGN=1 PRISM_BUNDLES=app npm run build
```

The open-source branch uses its checked-in online service configuration. No
endpoint override is needed. Desktop builds require network access to Prism
services. Signing and notarization are only needed when distributing installers
to end users.

## Development

From `desktop-app/`, install dependencies once and start Tauri in development
mode. The command rebuilds the Go sidecar with the online service configuration
before starting the desktop shell:

```sh
cd desktop-app
npm ci
npm run dev
```

### Package the desktop app locally

On macOS, build a runnable app without signing or creating a DMG:

```sh
cd desktop-app
PRISM_SKIP_CODESIGN=1 PRISM_BUNDLES=app npm run build
```

The generated app is under
`desktop-app/src-tauri/target/release/bundle/` for the current platform. A
signed installer requires platform signing and notarization configured by the
release owner.

### Checks

Run the maintained Hub test suite from the repository root:

```sh
./run_tests.sh
```

Run desktop checks:

```sh
cd desktop-app
npm ci
npm run check
```

Plugin development belongs in the corresponding plugin repository. See the [PluginBridge SDK](https://github.com/Prism-Rokid/prism-plugin-sdk) for protocol libraries and examples.

## Documentation

- [Architecture and public contract](docs/01-整体架构与公共契约.md)
- [Hub design](docs/04-Hub功能设计.md)
- [PluginBridge SDK guide](docs/06-SDK功能设计与使用指南.md)

## Contributing

Bug reports and pull requests are welcome. Please include reproduction steps for bugs and keep changes scoped to the Hub or Desktop code in this repository. Do not include account data, conversation content, credentials, local file paths, or screenshots containing personal information in issues or pull requests.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
