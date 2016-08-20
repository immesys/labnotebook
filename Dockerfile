FROM immesys/gobase

RUN go get github.com/immesys/labnotebook
ENTRYPOINT $GOPATH/bin/labnotebook
