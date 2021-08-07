FROM golang:1.17-alpine
LABEL author="Arifin Rais"
LABEL affiliation="Institut Teknologi Bandung"

# os setup
WORKDIR /app

COPY src/data-server .
COPY docker/data-server/.env .
RUN go mod download

RUN go build -o /docker-gs-ping

##
## Deploy
##

FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /docker-gs-ping /docker-gs-ping

EXPOSE 5000

USER nonroot:nonroot

ENTRYPOINT ["/docker-gs-ping"]
# run command