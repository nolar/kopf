ARG PYTHON_VERSION=3.14
ARG VARIANT=slim

#
# Builder: install dependencies and build the package into a venv.
# .git must be in context for setuptools-scm to determine the version.
#
FROM python:${PYTHON_VERSION}-${VARIANT} AS builder

# Conditional build deps: alpine uses apk, slim/bookworm uses apt-get.
RUN if command -v apk >/dev/null 2>&1; then \
        apk add --no-cache gcc g++ make linux-headers git; \
    else \
        apt-get update && apt-get install -y --no-install-recommends gcc g++ make git \
        && rm -rf /var/lib/apt/lists/*; \
    fi

COPY . /src
WORKDIR /src

RUN python -m venv /opt/kopf \
    && /opt/kopf/bin/pip install --no-cache-dir ".[full-auth,uvloop,dev]" \
    && /opt/kopf/bin/pip install --no-cache-dir \
        "oscrypto @ git+https://github.com/wbond/oscrypto.git@1547f535001ba568b239b8797465536759c742a3" \
    && rm -rf /src

#
# Final: minimal image with the pre-built venv.
#
FROM python:${PYTHON_VERSION}-${VARIANT}

COPY --from=builder /opt/kopf /opt/kopf
ENV PATH="/opt/kopf/bin:$PATH"

COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY docker/usage.txt /usr/local/share/kopf/usage.txt
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /app
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
