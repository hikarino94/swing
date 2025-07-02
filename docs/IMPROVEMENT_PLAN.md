# swingプロジェクト改善計画

## 概要
このドキュメントは、日本株分析ツールキット「swing」プロジェクトの改善提案と実装計画をまとめたものです。

## 現状の課題と改善提案

### 1. ✅ 最優先事項：テストカバレッジの向上（2% → 57%達成！）

#### 実施した改善
- インポートエラーを修正し、すべてのテストが実行可能に
- fetchモジュールとscreeningモジュールの包括的なテストを追加
- 共通フィクスチャをconftest.pyに集約
- テストカバレッジが**2%から57%に大幅改善**！

#### 残りの課題
- バックテストモジュールのテスト作成
- データベースモジュールのテスト作成
- UIモジュールのテスト作成
- 目標の80%カバレッジまでさらなる改善が必要

#### 改善案
```bash
# Phase 1: インポートエラーの修正
- tests/test_gui.py のインポートパス修正
- tests/ui/test_web.py のインポートパス修正
- その他の失敗テストの修正

# Phase 2: コアモジュールのテスト追加（目標カバレッジ80%）
- fetch/*.py: APIモックを使用したユニットテスト
- screening/*.py: テストデータを使用した検証
- backtest/*.py: 既知の結果に対する回帰テスト
- db/*.py: インメモリSQLiteを使用したテスト
```

#### 実装タスク
1. `tests/fixtures/` ディレクトリにテスト用データを準備
2. `tests/conftest.py` にpytestフィクスチャを定義
3. 各モジュールに対応するテストファイルを作成
4. GitHub ActionsのCIでカバレッジレポートを生成

### 2. 🔒 セキュリティの強化

#### 問題点
- 認証情報がJSONファイルに平文保存
- Flask SECRET_KEYがハードコーディング
- APIトークンの管理が不適切

#### 改善案
```python
# 環境変数を使用した認証情報管理
import os
from dotenv import load_dotenv

load_dotenv()

JQUANTS_MAIL = os.getenv('JQUANTS_MAIL')
JQUANTS_PASSWORD = os.getenv('JQUANTS_PASSWORD')
FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY', os.urandom(24).hex())
```

#### 実装タスク
1. `python-dotenv` パッケージを追加
2. `.env.example` ファイルを作成
3. 設定読み込みを環境変数優先に変更
4. Dockerを使用する場合のシークレット管理方法をドキュメント化

### 3. ⚡ パフォーマンス最適化

#### 問題点
- 17ファイルで`DataFrame.iterrows()`を使用（非効率）
- 大量データ処理時のメモリ使用量過多
- データベースクエリの最適化不足

#### 改善案
```python
# Before: iterrows()使用
for index, row in df.iterrows():
    process_row(row)

# After: ベクトル化
df['result'] = df.apply(process_row_vectorized, axis=1)
# または
results = process_dataframe_batch(df)
```

#### 実装タスク
1. `pandas_performance_optimizer.py` ユーティリティを作成
2. iterrows()をベクトル化操作に置換
3. チャンク処理の実装（大量データ対応）
4. SQLクエリにインデックスとバッチ処理を追加

### 4. 📊 統一的なログとエラーハンドリング

#### 問題点
- ログ使用の一貫性不足
- エラーハンドリングパターンの不統一
- デバッグ情報の不足

#### 改善案
```python
# 統一的なエラーハンドラー
from src.utils.error_handler import handle_error, SwingException

@handle_error
def fetch_data():
    try:
        # 処理
        pass
    except SpecificError as e:
        raise SwingException("データ取得に失敗", original_error=e)
```

#### 実装タスク
1. `src/utils/error_handler.py` を作成
2. カスタム例外クラスの階層を定義
3. 全モジュールで統一的なログ設定を使用
4. エラー時の自動通知機能を追加（オプション）

### 5. 📚 ドキュメントの充実

#### 問題点
- APIリファレンスの不足
- アーキテクチャ設計書の欠如
- トラブルシューティングガイドなし

#### 改善案
```markdown
docs/
├── api/                 # APIリファレンス（Sphinx自動生成）
├── architecture/        # システム設計書
├── tutorials/          # チュートリアル
├── troubleshooting/    # トラブルシューティング
└── development/        # 開発者ガイド
```

#### 実装タスク
1. Sphinxを使用したAPIドキュメント自動生成
2. アーキテクチャ図の作成（PlantUMLまたはMermaid）
3. よくある問題と解決方法のドキュメント化
4. 開発環境セットアップガイドの詳細化

### 6. 🔄 CI/CDパイプラインの強化

#### 現状
- 基本的なGitHub Actionsは設定済み
- デプロイプロセスが手動

#### 改善案
```yaml
# .github/workflows/ci.yml の拡張
- テストカバレッジレポート
- パフォーマンステスト
- セキュリティスキャン（Safety、Bandit）
- 自動リリースノート生成
- Dockerイメージのビルドとプッシュ
```

### 7. 🎨 UIの改善

#### Web UI
- リアルタイムデータ更新（WebSocket）
- チャート表示の高度化（Chart.jsまたはPlotly）
- ダークモード対応
- モバイルレスポンシブの改善

#### レガシーGUI
- 段階的な廃止計画（Web UIへの完全移行）

### 8. 📦 依存関係の最適化

#### 実装タスク
1. 未使用パッケージの特定と削除
2. バージョン固定（requirements.lockファイル）
3. 軽量な代替パッケージの検討

## 実装優先順位とスケジュール

### Phase 1（1-2週間）: 基礎的な問題の修正
- [ ] テストのインポートエラー修正
- [ ] セキュリティ: 環境変数対応
- [ ] 基本的なユニットテスト追加（カバレッジ30%目標）

### Phase 2（2-3週間）: コア機能の改善
- [ ] パフォーマンス最適化（iterrows()の置換）
- [ ] エラーハンドリングの統一化
- [ ] ログ機能の完全実装

### Phase 3（3-4週間）: 品質向上
- [ ] テストカバレッジ80%達成
- [ ] ドキュメント整備
- [ ] CI/CDパイプライン強化

### Phase 4（継続的）: 機能拡張
- [ ] Web UIの高度化
- [ ] 新しいスクリーニング手法の追加
- [ ] バックテスト機能の拡張

## 作業を始めるために

1. このドキュメントの各セクションは独立して実装可能
2. 各改善案には具体的なコード例を含む
3. 実装タスクは明確で測定可能
4. 優先順位に従って段階的に実装

## 成功指標

- テストカバレッジ: 2% → 80%以上
- パフォーマンス: データ処理速度50%向上
- セキュリティ: 全認証情報の環境変数化
- ドキュメント: APIリファレンス100%カバー
- エラー率: 本番環境でのエラー50%削減
