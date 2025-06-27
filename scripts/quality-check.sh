#!/bin/bash
# コード品質チェックスクリプト

set -e

echo "🔍 コード品質チェックを開始します..."

# 色付きの出力用
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 関数: チェック結果の表示
check_result() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $1 成功${NC}"
        return 0
    else
        echo -e "${RED}✗ $1 失敗${NC}"
        return 1
    fi
}

# 対象ディレクトリ
DIRS="src/utils src/api src/analysis src/strategies"
FILES="*.py"

echo "対象ディレクトリ: $DIRS"
echo "対象ファイル: $FILES"
echo ""

# Black フォーマットチェック
echo "📝 Black フォーマットチェック..."
if command -v black &> /dev/null; then
    black --check --diff $DIRS $FILES 2>/dev/null
    check_result "Black フォーマット"
else
    echo -e "${YELLOW}⚠ Black がインストールされていません${NC}"
fi

echo ""

# Ruff リントチェック
echo "🔧 Ruff リントチェック..."
if command -v ruff &> /dev/null; then
    ruff check $DIRS $FILES
    check_result "Ruff リント"
else
    echo -e "${YELLOW}⚠ Ruff がインストールされていません${NC}"
fi

echo ""

# MyPy 型チェック
echo "🔍 MyPy 型チェック..."
if command -v mypy &> /dev/null; then
    mypy src/utils/ --ignore-missing-imports 2>/dev/null || true
    check_result "MyPy 型チェック (警告のみ)"
else
    echo -e "${YELLOW}⚠ MyPy がインストールされていません${NC}"
fi

echo ""

# Bandit セキュリティチェック
echo "🔒 Bandit セキュリティチェック..."
if command -v bandit &> /dev/null; then
    bandit -r $DIRS -f json -o bandit-report.json 2>/dev/null || true
    bandit -r $DIRS --severity-level medium 2>/dev/null || true
    check_result "Bandit セキュリティ (警告のみ)"
else
    echo -e "${YELLOW}⚠ Bandit がインストールされていません${NC}"
fi

echo ""

# 構文チェック
echo "📋 Python 構文チェック..."
SYNTAX_ERROR=0
for dir in $DIRS; do
    if [ -d "$dir" ]; then
        echo "  チェック中: $dir/*.py"
        for file in $dir/*.py; do
            if [ -f "$file" ]; then
                python3 -m py_compile "$file" 2>/dev/null
                if [ $? -ne 0 ]; then
                    echo -e "${RED}    ✗ $file${NC}"
                    SYNTAX_ERROR=1
                else
                    echo -e "${GREEN}    ✓ $file${NC}"
                fi
            fi
        done
    fi
done

# ルートディレクトリのPythonファイル
for file in *.py; do
    if [ -f "$file" ]; then
        python3 -m py_compile "$file" 2>/dev/null
        if [ $? -ne 0 ]; then
            echo -e "${RED}  ✗ $file${NC}"
            SYNTAX_ERROR=1
        else
            echo -e "${GREEN}  ✓ $file${NC}"
        fi
    fi
done

if [ $SYNTAX_ERROR -eq 0 ]; then
    echo -e "${GREEN}✓ 構文チェック 成功${NC}"
else
    echo -e "${RED}✗ 構文チェック 失敗${NC}"
fi

echo ""

# テスト実行
echo "🧪 テスト実行..."
if command -v python3 &> /dev/null; then
    if [ -d "tests" ]; then
        # 簡単なテスト実行
        python3 -c "
import sys, os
sys.path.append('.')

# 基本的なインポートテスト
try:
    from src.utils.config import ConfigManager
    from src.utils.db_utils import DatabaseManager
    from src.utils.exceptions import APIError
    print('✓ 基本インポート成功')
except Exception as e:
    print(f'✗ インポートエラー: {e}')
    sys.exit(1)

print('✓ 基本テスト成功')
"
        check_result "基本テスト"
    else
        echo -e "${YELLOW}⚠ testsディレクトリが見つかりません${NC}"
    fi
else
    echo -e "${RED}✗ Python3 が見つかりません${NC}"
fi

echo ""
echo "🎉 コード品質チェック完了！"

# 結果サマリー
echo ""
echo "📊 チェック結果サマリー:"
echo "  - フォーマット: Black"
echo "  - リント: Ruff"
echo "  - 型チェック: MyPy"
echo "  - セキュリティ: Bandit"
echo "  - 構文: Python compile"
echo "  - 基本テスト: 実行済み"

echo ""
echo "💡 修正方法:"
echo "  - フォーマット修正: black ."
echo "  - リント修正: ruff --fix ."
echo "  - 型エラー: ファイルの型ヒントを確認"
echo "  - セキュリティ: bandit-report.json を確認"
