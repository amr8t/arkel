# syntax=docker/dockerfile:1
#
# Runtime image for arkel. Packages the checksum-verified prebuilt release
# binary, so no Rust toolchain is needed to build the image.
#
#   docker build --build-arg VERSION=v0.0.5 -t arkel .
#   docker buildx build --platform linux/amd64,linux/arm64 --build-arg VERSION=v0.0.5 -t arkel .
#
# `VERSION=latest` (default) tracks the newest GitHub release.

FROM debian:bookworm-slim

ARG VERSION=latest
ARG TARGETARCH

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# Fetch the per-arch asset and verify its sha256. amd64 falls back to the legacy
# single `arkel` asset for releases before per-arch assets existed.
RUN set -eux; \
    arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "${arch}" in \
      amd64) asset="arkel-x86_64" ;; \
      arm64) asset="arkel-aarch64" ;; \
      *) echo "unsupported arch: ${arch}" >&2; exit 1 ;; \
    esac; \
    if [ "${VERSION}" = "latest" ]; then \
      base="https://github.com/amr8t/arkel/releases/latest/download"; \
    else \
      base="https://github.com/amr8t/arkel/releases/download/${VERSION}"; \
    fi; \
    if ! curl -fsSL -o /usr/local/bin/arkel "${base}/${asset}"; then \
      if [ "${asset}" = "arkel-x86_64" ]; then \
        asset="arkel"; \
        curl -fsSL -o /usr/local/bin/arkel "${base}/${asset}"; \
      else \
        echo "no ${asset} asset at ${base}" >&2; exit 1; \
      fi; \
    fi; \
    curl -fsSL -o /tmp/arkel.sha256 "${base}/${asset}.sha256"; \
    echo "$(awk '{print $1}' /tmp/arkel.sha256)  /usr/local/bin/arkel" | sha256sum -c -; \
    rm -f /tmp/arkel.sha256; \
    chmod 0755 /usr/local/bin/arkel

RUN useradd --system --home-dir /var/lib/arkel --shell /usr/sbin/nologin arkel \
 && mkdir -p /var/lib/arkel /etc/arkel \
 && chown -R arkel:arkel /var/lib/arkel

VOLUME ["/var/lib/arkel"]
# iroh QUIC endpoint (UDP).
EXPOSE 9001/udp
USER arkel

ENTRYPOINT ["/usr/local/bin/arkel"]
CMD ["storage", "--config", "/etc/arkel/arkel-node.toml"]
