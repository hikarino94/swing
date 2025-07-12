"""
リポジトリインターフェースの定義
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Any

import pandas as pd


class BaseRepository(ABC):
    """リポジトリの基底インターフェース"""

    @abstractmethod
    def connect(self) -> None:
        """データベースに接続"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """データベース接続を切断"""
        pass

    @abstractmethod
    def begin_transaction(self) -> None:
        """トランザクションを開始"""
        pass

    @abstractmethod
    def commit(self) -> None:
        """トランザクションをコミット"""
        pass

    @abstractmethod
    def rollback(self) -> None:
        """トランザクションをロールバック"""
        pass


class PriceRepository(BaseRepository):
    """株価データのリポジトリインターフェース"""

    @abstractmethod
    def find_by_code_and_date_range(
        self, code: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """指定銘柄・期間の株価データを取得"""
        pass

    @abstractmethod
    def find_latest_by_code(self, code: str) -> dict[str, Any] | None:
        """指定銘柄の最新株価を取得"""
        pass

    @abstractmethod
    def find_all_by_date(self, target_date: date) -> pd.DataFrame:
        """指定日の全銘柄の株価データを取得"""
        pass

    @abstractmethod
    def save_batch(self, data: pd.DataFrame) -> int:
        """株価データを一括保存"""
        pass

    @abstractmethod
    def delete_by_date_range(self, start_date: date, end_date: date) -> int:
        """指定期間のデータを削除"""
        pass

    @abstractmethod
    def get_latest_date(self) -> date | None:
        """最新のデータ日付を取得"""
        pass


class ListedInfoRepository(BaseRepository):
    """上場企業情報のリポジトリインターフェース"""

    @abstractmethod
    def find_by_code(self, code: str) -> dict[str, Any] | None:
        """指定銘柄の企業情報を取得"""
        pass

    @abstractmethod
    def find_all_active(self) -> pd.DataFrame:
        """削除フラグが立っていない全企業情報を取得"""
        pass

    @abstractmethod
    def find_by_sector(self, sector_code: str) -> pd.DataFrame:
        """指定セクターの企業情報を取得"""
        pass

    @abstractmethod
    def save_batch(self, data: pd.DataFrame) -> int:
        """企業情報を一括保存"""
        pass

    @abstractmethod
    def mark_as_deleted(self, codes: list[str]) -> int:
        """指定銘柄に削除フラグを設定"""
        pass

    @abstractmethod
    def update_delete_flags(self, active_codes: list[str]) -> int:
        """アクティブな銘柄以外に削除フラグを設定"""
        pass


class StatementsRepository(BaseRepository):
    """財務諸表データのリポジトリインターフェース"""

    @abstractmethod
    def find_by_code_and_period(
        self, code: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """指定銘柄・期間の財務諸表を取得"""
        pass

    @abstractmethod
    def find_latest_by_code(self, code: str) -> dict[str, Any] | None:
        """指定銘柄の最新財務諸表を取得"""
        pass

    @abstractmethod
    def find_by_disclosure_date(self, disclosure_date: date) -> pd.DataFrame:
        """指定開示日の財務諸表を取得"""
        pass

    @abstractmethod
    def save_batch(self, data: pd.DataFrame) -> int:
        """財務諸表データを一括保存"""
        pass

    @abstractmethod
    def get_latest_disclosure_date(self) -> date | None:
        """最新の開示日を取得"""
        pass

    @abstractmethod
    def find_quarterly_statements(
        self, codes: list[str], lookback_days: int
    ) -> pd.DataFrame:
        """四半期決算データを取得"""
        pass


class SignalRepository(BaseRepository):
    """シグナルデータのリポジトリインターフェース"""

    @abstractmethod
    def find_fundamental_signals(
        self, start_date: date, end_date: date, signal_types: list[str] | None = None
    ) -> pd.DataFrame:
        """ファンダメンタルシグナルを取得"""
        pass

    @abstractmethod
    def find_technical_signals(
        self, start_date: date, end_date: date, indicators: list[str] | None = None
    ) -> pd.DataFrame:
        """テクニカルシグナルを取得"""
        pass

    @abstractmethod
    def save_fundamental_signals(self, signals: pd.DataFrame) -> int:
        """ファンダメンタルシグナルを保存"""
        pass

    @abstractmethod
    def save_technical_signals(self, signals: pd.DataFrame) -> int:
        """テクニカルシグナルを保存"""
        pass

    @abstractmethod
    def delete_old_signals(self, cutoff_date: date, signal_type: str) -> int:
        """古いシグナルを削除"""
        pass


class IndicatorRepository(BaseRepository):
    """テクニカル指標データのリポジトリインターフェース"""

    @abstractmethod
    def find_by_code_and_date_range(
        self,
        code: str,
        start_date: date,
        end_date: date,
        indicators: list[str] | None = None,
    ) -> pd.DataFrame:
        """指定銘柄・期間のテクニカル指標を取得"""
        pass

    @abstractmethod
    def save_batch(self, data: pd.DataFrame) -> int:
        """テクニカル指標データを一括保存"""
        pass

    @abstractmethod
    def get_available_indicators(self) -> list[str]:
        """利用可能な指標名のリストを取得"""
        pass

    @abstractmethod
    def delete_by_date_range(self, start_date: date, end_date: date) -> int:
        """指定期間のデータを削除"""
        pass
