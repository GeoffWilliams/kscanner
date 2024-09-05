# kscanner

Kafka Scanner - scan the contents of a topic to see what type of messages are contained

## Build

Build is static with musl to avoid glbc version compatibilty errors

```shell
sudo apt install musl musl-dev musl-tools
make build
```