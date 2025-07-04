#!/bin/bash
echo "========================================="
echo "WSL2 ネットワーク診断情報"
echo "========================================="
echo ""

echo "1. WSL2情報:"
echo "   ディストリビューション: $WSL_DISTRO_NAME"
cat /etc/os-release | grep PRETTY_NAME
echo ""

echo "2. ネットワークインターフェース:"
ip addr show eth0
echo ""

echo "3. MTU情報:"
ip link show eth0 | grep mtu
echo ""

echo "4. ルーティングテーブル:"
ip route
echo ""

echo "5. DNS設定:"
cat /etc/resolv.conf
echo ""

echo "6. 現在のプロセス:"
ps aux | grep -E "(python|flask)" | grep -v grep
echo ""

echo "7. ポート使用状況:"
ss -tlnp | grep -E "(5000|8080)"
echo ""

echo "8. システムリソース:"
free -h
echo ""

echo "9. ディスク使用状況:"
df -h | grep -E "(/$|/home)"
echo ""

echo "========================================="
echo "推奨される対処法:"
echo "1. MTUサイズの変更: sudo ip link set dev eth0 mtu 1450"
echo "2. Waitressサーバーの使用: python src/ui/web_production.py"
echo "3. テストサーバーで検証: python test_network.py"
echo "========================================="
