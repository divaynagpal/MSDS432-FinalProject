# Use an official Go runtime as a parent image

FROM golang:1.22.2 AS build

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files to the workspace
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source code to the workspace
COPY . .

# Build the Go application

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app ./controller

# Use a minimal base image to reduce the image size
FROM alpine:latest  

# Set the Current Working Directory inside the container
WORKDIR /root/

# Copy the Pre-built binary file from the previous stage
COPY --from=build /app/app .

# Expose port 8080 to the outside world
EXPOSE 8080

# Command to run the executable

CMD ["./app"]