FROM    golang:1.25-alpine
WORKDIR /app
COPY    . .
RUN     go build -mod=vendor -o server
RUN     apk update && apk add curl
EXPOSE  8080
ENTRYPOINT     ["./server"]
