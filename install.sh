apt-get update && apt-get install -y unzip libprotobuf-dev libssl-dev

set -o errexit -o verbose

install_protoc() {
    local download_path="/tmp/protoc.zip"
    local url="https://github.com/protocolbuffers/protobuf/releases/download/v3.19.5/protoc-3.19.5-linux-x86_64.zip"

    curl -fsSL "${url}" -o "${download_path}"

    unzip -qq "${download_path}" -d "/tmp"

    mv --force --verbose "/tmp/bin/protoc" "/usr/bin"
}

install_protoc
