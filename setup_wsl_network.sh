#!/bin/bash
# WSL2ネットワーク設定スクリプト

echo "WSL2ネットワーク設定を行います..."

# MTUサイズの変更
echo "MTUサイズを1450に変更しています..."
sudo ip link set dev eth0 mtu 1450

# 設定ファイルの作成
echo "WSL2自動設定ファイルを作成しています..."
sudo tee /etc/wsl.conf << 'EOF'
[boot]
command = /bin/sh -c "ip link set dev eth0 mtu 1450"

[network]
generateResolvConf = true
generateHosts = true
EOF

echo ""
echo "設定が完了しました！"
echo "現在のMTUサイズ:"
ip link show eth0 | grep mtu

echo ""
echo "注意: WSL2を再起動すると、自動的にMTUサイズが1450に設定されます。"
