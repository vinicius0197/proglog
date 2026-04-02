### Compiling protobufs

```
go get google.golang.org/protobuf/...@v1.25.0
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
export PATH="$PATH:$(go env GOPATH)/bin"

make
```