#!/usr/bin/env python
"""Update idtoken.json using J-Quants authentication API."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import requests

API_AUTH = "https://api.jquants.com/v1/token/auth_user"
API_REFRESH = "https://api.jquants.com/v1/token/auth_refresh"
DEFAULT_ACCOUNT = "account.json"


def _auth_user(mail: str, password: str) -> str:
    """Return ``refreshToken`` by authenticating with mail and password."""
    resp = requests.post(
        API_AUTH,
        json={"mailaddress": mail, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "refreshToken" not in data:
        raise RuntimeError("refreshToken not found in response")
    return data["refreshToken"]


def _load_account(path: str) -> tuple[str, str, str]:
    """Return ``(mail, password, password_hash)`` from ``path`` if it exists."""
    p = Path(path)
    if not p.is_file():
        p = Path(__file__).resolve().parent / path
    if p.is_file():
        with p.open("r", encoding="utf-8") as f:
            js = json.load(f)
        return js.get("mail", ""), js.get("password", ""), js.get("password_hash", "")
    return "", "", ""


def _get_id_token(refresh_token: str) -> str:
    """Return ``idToken`` using ``refresh_token``."""
    resp = requests.post(
        API_REFRESH,
        params={"refreshtoken": refresh_token},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "idToken" not in data:
        raise RuntimeError("idToken not found in response")
    return data["idToken"]


def update(mail: str, password: str, outfile: str) -> str:
    """Obtain a new ``idToken`` and write it to ``outfile``."""
    refresh = _auth_user(mail, password)
    token = _get_id_token(refresh)
    path = Path(outfile)
    with path.open("w", encoding="utf-8") as f:
        json.dump({"idToken": token}, f)
    return token


def _cli() -> None:
    ap = argparse.ArgumentParser(description="update idtoken.json")
    ap.add_argument("--mail", help="registered email")
    ap.add_argument("--password", help="login password")
    ap.add_argument("--account", default=DEFAULT_ACCOUNT, help="credential file")
    ap.add_argument("--out", default="idtoken.json", help="output file")
    a = ap.parse_args()

    mail, pwd = a.mail, a.password
    if not mail or not pwd:
        m, p, _ = _load_account(a.account)
        mail = mail or m
        pwd = pwd or p
    if not mail or not pwd:
        ap.error("mail and password are required")
    update(mail, pwd, a.out)


if __name__ == "__main__":
    _cli()
