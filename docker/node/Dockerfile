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

# Install K3s agent binary (don't start it yet)
RUN curl -sfL https://get.k3s.io | INSTALL_K3S_SKIP_START=true INSTALL_K3S_EXEC="agent" sh -

# Bootstrap script will be injected by provider at runtime via cloud-init/user-data
# The script connects to Netbird VPN and joins K8s cluster

CMD ["/bin/bash", "-c", "echo 'GPUScale node ready. Bootstrap via cloud-init.'; sleep infinity"]
