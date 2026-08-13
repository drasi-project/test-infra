FROM debian:bookworm-slim

ARG TARGETARCH
ARG DRASI_SERVER_VERSION=latest

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libssl3 \
    libpq5 \
    libjq1 \
    libonig5 \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) asset_arch="x86_64-linux-gnu" ;; \
      arm64) asset_arch="aarch64-linux-gnu" ;; \
      *) echo "Unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL \
      "https://github.com/drasi-project/drasi-server/releases/download/${DRASI_SERVER_VERSION}/drasi-server-${asset_arch}" \
      -o /usr/local/bin/drasi-server; \
    chmod +x /usr/local/bin/drasi-server

WORKDIR /app
RUN mkdir -p /app/config

EXPOSE 8080 50051

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["drasi-server"]
