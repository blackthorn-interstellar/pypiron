# Use a Rust base image
FROM rust:1.76 as builder

# Create a new empty shell project
WORKDIR /usr/src/pypiron
COPY . .

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install necessary runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary
COPY --from=builder /usr/src/pypiron/target/release/pypi-server /usr/local/bin/pypi-server

# Set the startup command
CMD ["pypi-server"]

# Expose port 80
EXPOSE 80 