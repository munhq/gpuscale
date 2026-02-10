FROM nvidia/cuda:12.6.0-runtime-ubuntu24.04

# Pre-install everything to minimize boot time
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    jq \
    rclone \
    iproute2 \
    openssh-server \
    && rm -rf /var/lib/apt/lists/*

# Install Netbird
RUN curl -fsSL https://pkgs.netbird.io/install.sh | sh

# Install K3s binary (agent only, don't start)
RUN curl -sfL https://get.k3s.io | INSTALL_K3S_SKIP_START=true INSTALL_K3S_EXEC="agent" sh -

# Copy bootstrap script
COPY docker/node/bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh

ENTRYPOINT ["/bootstrap.sh"]
