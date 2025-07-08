#!/bin/bash
# WSL2ネットワーク問題修正スクリプト

echo "WSL2ネットワーク設定を修正します..."

# 現在のMTUサイズを確認
current_mtu=$(ip link show eth0 | grep -oP 'mtu \K\d+')
echo "現在のMTUサイズ: $current_mtu"

if [ "$current_mtu" -lt 1450 ]; then
    echo "MTUサイズが小さすぎます。1500に変更します..."
    sudo ip link set dev eth0 mtu 1500

    # 変更後の確認
    new_mtu=$(ip link show eth0 | grep -oP 'mtu \K\d+')
    echo "新しいMTUサイズ: $new_mtu"

    if [ "$new_mtu" -eq 1500 ]; then
        echo "✓ MTUサイズの変更に成功しました"
    else
        echo "⚠ MTUサイズの変更に失敗しました"
        echo "手動で以下のコマンドを実行してください："
        echo "sudo ip link set dev eth0 mtu 1500"
    fi
else
    echo "MTUサイズは適切です ($current_mtu)"
fi

echo ""
echo "IPアドレス情報:"
ip addr show eth0 | grep inet | grep -v inet6

echo ""
echo "Flaskアプリケーションを起動するには："
echo "python -m src.ui.web"
