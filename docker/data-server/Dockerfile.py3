FROM golang:1.17-alpine
LABEL author="Arifin Rais"
LABEL affiliation="Institut Teknologi Bandung"

# os setup
WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY src/data-server/data_server.go .

RUN go build -o /docker-gs-ping

##
## Deploy
##

FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /docker-gs-ping /docker-gs-ping

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/docker-gs-ping"]
# run command