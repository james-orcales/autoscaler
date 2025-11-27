FROM    golang:1.25-alpine
WORKDIR /app
COPY    . .
RUN     go build -mod=vendor -o workload
ENTRYPOINT ["./workload"]
