FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium python3 python3-pip ca-certificates \
 && pip3 install --break-system-packages websocket-client requests \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /work
CMD ["python3", "/work/vids_keepalive.py"]
