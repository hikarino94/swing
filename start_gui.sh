#!/bin/bash

# WSL環境でGUIアプリケーションを起動するスクリプト

# Windowsホストの IP アドレスを取得
WINDOWS_HOST=$(grep nameserver /etc/resolv.conf | awk '{print $2; exit}')

# DISPLAY環境変数を設定
export DISPLAY=${WINDOWS_HOST}:0.0
export LIBGL_ALWAYS_INDIRECT=1

echo "DISPLAY設定: $DISPLAY"
echo "GUI起動中..."

# GUIアプリケーションを起動
python3 desktop/gui.py
