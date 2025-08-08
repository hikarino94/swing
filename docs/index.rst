.. Swing Trade Analysis Tool documentation master file

Swing Trade Analysis Tool ドキュメント
======================================

日本株スイングトレード分析ツールへようこそ！

このツールは、J-Quants APIを使用して日本株のスイングトレード戦略を開発・検証するためのPythonベースのツールキットです。

.. toctree::
   :maxdepth: 2
   :caption: 目次:

   getting_started
   installation
   configuration
   modules/index
   api/index
   development
   changelog

主な機能
--------

* **データ取得**: J-Quants APIからの自動データ取得
* **スクリーニング**: ファンダメンタル、テクニカル、機械学習ベースの銘柄選定
* **バックテスト**: 戦略の過去パフォーマンス検証
* **GUI/Web UI**: TkinterおよびFlaskベースのユーザーインターフェース
* **自動化**: スケジューラーによる定期的なデータ更新

クイックスタート
----------------

1. 環境構築::

    python setup_environment.py

2. 認証情報の設定::

    cp account.json.example account.json
    # account.jsonを編集して認証情報を入力

3. トークンの取得::

    python update_idtoken.py

4. データの取得::

    python fetch/daily_quotes.py
    python fetch/listed_info.py
    python fetch/statements.py

インデックスと検索
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`