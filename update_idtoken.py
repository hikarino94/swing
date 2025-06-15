#!/usr/bin/env python
"""Update idtoken.json using J-Quants authentication API."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import requests

API_AUTH = "https://api.jquants.com/v1/token/auth_user"
API_REFRESH = "https://api.jquants.com/v1/token/auth_refresh"


def _get_refresh_token(mail: str, password: str) -> str:
    """Return refreshToken by authenticating with email and password."""
    resp = requests.post(
        API_AUTH,
        json={"mailaddress": mail, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    js = resp.json()
    if "refreshToken" not in js:
        raise RuntimeError("refreshToken not found in response")
    return js["refreshToken"]


def _get_id_token(refresh_token: str) -> str:
    """Return idToken using a refresh token."""
    resp = requests.post(
        API_REFRESH,
        json={"refreshToken": refresh_token},
        timeout=30,
    )
    resp.raise_for_status()
    js = resp.json()
    if "idToken" not in js:
        raise RuntimeError("idToken not found in response")
    return js["idToken"]


def update(mail: str, password: str, outfile: str) -> str:
    """Update idtoken.json and return the token."""
    ref = _get_refresh_token(mail, password)
    token = _get_id_token(ref)
    path = Path(outfile)
    with path.open("w", encoding="utf-8") as f:
        json.dump({"idToken": token}, f)
    return token


def _cli() -> None:
    ap = argparse.ArgumentParser(description="update idtoken.json")
    ap.add_argument("--mail", default="example@example.com", help="registered email")
    ap.add_argument("--password", default="password", help="login password")
    ap.add_argument("--out", default="idtoken.json", help="output file")
    a = ap.parse_args()
    update(a.mail, a.password, a.out)


if __name__ == "__main__":
    _cli()
