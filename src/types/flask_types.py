"""Flask関連の型定義と型ガード関数"""

from typing import TYPE_CHECKING, Any

from flask import Request as FlaskRequest
from flask_login import UserMixin
from werkzeug.datastructures import FileStorage

if TYPE_CHECKING:
    pass


class User(UserMixin):
    """ユーザーモデルの型定義"""

    id: int
    email: str
    username: str


class RequestWithUser(FlaskRequest):
    """current_user属性を持つRequest型"""

    current_user: User


def get_form_value(request: FlaskRequest, key: str, default: str = "") -> str:
    """
    request.formから安全に値を取得する型ガード関数

    Args:
        request: Flaskのrequestオブジェクト
        key: 取得するフォームフィールドのキー
        default: デフォルト値

    Returns:
        フォームの値またはデフォルト値
    """
    if request.form is not None:
        return request.form.get(key, default)  # type: ignore[no-any-return]
    return default


def get_json_value(request: FlaskRequest, key: str, default: Any = None) -> Any:
    """
    request.jsonから安全に値を取得する型ガード関数

    Args:
        request: Flaskのrequestオブジェクト
        key: 取得するJSONフィールドのキー
        default: デフォルト値

    Returns:
        JSONの値またはデフォルト値
    """
    if request.json is not None and isinstance(request.json, dict):
        return request.json.get(key, default)
    return default


def get_args_value(request: FlaskRequest, key: str, default: str = "") -> str:
    """
    request.argsから安全に値を取得する型ガード関数

    Args:
        request: Flaskのrequestオブジェクト
        key: 取得するクエリパラメータのキー
        default: デフォルト値

    Returns:
        クエリパラメータの値またはデフォルト値
    """
    if request.args is not None:
        return request.args.get(key, default)  # type: ignore[no-any-return]
    return default


def get_form_array(request: FlaskRequest, key: str) -> list[str]:
    """
    request.formから配列値を安全に取得する

    Args:
        request: Flaskのrequestオブジェクト
        key: 取得するフォームフィールドのキー

    Returns:
        フォームの配列値または空リスト
    """
    if request.form is not None:
        return request.form.getlist(key)  # type: ignore[no-any-return]
    return []


def has_form_key(request: FlaskRequest, key: str) -> bool:
    """
    request.formに特定のキーが存在するかチェック

    Args:
        request: Flaskのrequestオブジェクト
        key: チェックするキー

    Returns:
        キーが存在する場合True
    """
    if request.form is not None:
        return key in request.form
    return False


def has_json_key(request: FlaskRequest, key: str) -> bool:
    """
    request.jsonに特定のキーが存在するかチェック

    Args:
        request: Flaskのrequestオブジェクト
        key: チェックするキー

    Returns:
        キーが存在する場合True
    """
    if request.json is not None and isinstance(request.json, dict):
        return key in request.json
    return False


def get_file(request: FlaskRequest, key: str) -> FileStorage | None:
    """
    request.filesから安全にファイルを取得

    Args:
        request: Flaskのrequestオブジェクト
        key: ファイルフィールドのキー

    Returns:
        FileStorageオブジェクトまたはNone
    """
    if request.files is not None:
        return request.files.get(key)
    return None
