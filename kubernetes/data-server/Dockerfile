FROM golang:1.16-alpine
LABEL author="Arifin Rais"
LABEL affiliation="Institut Teknologi Bandung"

WORKDIR /go/src/app

COPY src/data-server .
COPY kubernetes/data-server/.env .
RUN go mod tidy
RUN go build -o /data-server

EXPOSE 5000

ENTRYPOINT ["/data-server"]