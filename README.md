# updates-provider

## Build
This project uses 'wx' cargo registry. To build, first add the following to your `~/.cargo/config` file:
```toml
[net]
git-fetch-with-cli = true

[registries.wx]
index = "https://gitlab.waves.exchange/we-private/alexandrie.git"
```
 Then build as usual: `cargo build`
 