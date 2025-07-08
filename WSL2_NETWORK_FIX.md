# WSL2 ネットワーク問題の解決方法

## 問題の原因
現在のWSL2環境でMTUサイズが1280と非常に小さく設定されているため、大きなデータの送信時にパケットが過度に分割され、タイムアウトが発生しています。

## 解決方法

### 1. 一時的な解決（WSL2内で実行）
```bash
# MTUサイズを1500に変更
sudo ip link set dev eth0 mtu 1500
```

### 2. 永続的な解決 - .wslconfigの設定

Windowsのユーザーフォルダ（C:\Users\[ユーザー名]）に `.wslconfig` ファイルを作成：

```ini
[wsl2]
memory=4GB
processors=2
localhostForwarding=true
nestedVirtualization=true

[experimental]
# ネットワーク関連の設定
networkingMode=mirrored
dnsTunneling=true
firewall=true
autoProxy=true
```

### 3. WSL2起動時の自動設定

`/etc/wsl.conf` ファイルを作成または編集：
```bash
sudo nano /etc/wsl.conf
```

以下の内容を追加：
```ini
[boot]
command = /bin/sh -c "ip link set dev eth0 mtu 1500"

[network]
generateResolvConf = true
```

### 4. Windowsファイアウォールの設定確認

PowerShellを管理者権限で実行：
```powershell
# WSL2のファイアウォールルールを追加
New-NetFirewallRule -DisplayName "WSL2 Flask App" -Direction Inbound -LocalPort 5000 -Protocol TCP -Action Allow
```

### 5. アプリケーション側の対策

#### 開発環境での起動方法
```bash
# 通常の開発モード（デバッグ有効）
python -m src.ui.web
```

#### 本番環境での起動方法（推奨）
```bash
# Waitressサーバーを使用（より安定）
pip install waitress
python src/ui/web_production.py
```

#### 軽量テストサーバーでの検証
```bash
# ネットワーク問題の診断
python dev_tools/test_network.py
```

### 6. トラブルシューティング

#### MTUサイズの確認
```bash
# WSL2内
ip link show eth0 | grep mtu

# Windows側（PowerShell）
netsh interface ipv4 show subinterfaces
```

#### ネットワーク接続の確認
```bash
# WSL2のIPアドレス確認
ip addr show eth0

# Windows側からWSL2へのping確認
# PowerShellで実行
wsl hostname -I
ping [上記で表示されたIPアドレス]
```

### 7. それでも解決しない場合

1. **WSL2の再起動**
   ```powershell
   wsl --shutdown
   wsl
   ```

2. **Windows側のネットワークアダプタのリセット**
   ```powershell
   # 管理者権限で実行
   netsh int ip reset
   netsh winsock reset
   ```

3. **Hyper-Vの仮想スイッチ設定確認**
   - Hyper-Vマネージャーを開く
   - 仮想スイッチマネージャーで「WSL」スイッチの設定を確認

## 推奨される使用方法

1. まず `test_network.py` を起動して、どのサイズのデータでタイムアウトするか確認
2. MTUサイズを1500に変更して再テスト
3. 問題が解決したら、永続的な設定を適用
4. 本番環境では `web_production.py` を使用して起動

## 注意事項

- WSL2のバージョンによって設定方法が異なる場合があります
- VPN使用時はMTUサイズをさらに小さくする必要がある場合があります
- Tailscaleなどのオーバーレイネットワークを使用している場合は、そちらのMTU設定も確認してください
