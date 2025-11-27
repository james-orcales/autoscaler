FROM    golang:1.25-alpine
WORKDIR /app
COPY    . .
RUN     go build -mod=vendor -o worker
EXPOSE  9090
ENTRYPOINT     ["./worker"]
