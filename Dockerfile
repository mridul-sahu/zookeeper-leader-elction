FROM golang

RUN go get -v github.com/mridul-sahu/zookeeper-leader-election

WORKDIR /go/src/github.com/mridul-sahu/zookeeper-leader-election

COPY . .

RUN go get -v

CMD [ "zookeeper-leader-election" ]