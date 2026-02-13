FROM nvidia/cuda:12.6.0-runtime-ubuntu24.04

# Install tools
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    jq \
    rclone \
    iproute2 \
    openssh-server \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Netbird VPN client
RUN curl -fsSL https://pkgs.netbird.io/install.sh | sh

# Install K3s binary manually (avoid systemd issues in Docker build)
RUN curl -sfL https://github.com/k3s-io/k3s/releases/download/v1.31.4%2Bk3s1/k3s -o /usr/local/bin/k3s \
    && chmod +x /usr/local/bin/k3s

# Bootstrap script will be injected by provider at runtime via cloud-init/user-data
# The script connects to Netbird VPN and joins K8s cluster

CMD ["/bin/bash", "-c", "echo 'GPUScale node ready. Bootstrap via cloud-init.'; sleep infinity"]
