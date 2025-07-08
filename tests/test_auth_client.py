"""認証済みクライアントのデバッグテスト"""


def test_authenticated_client_session(authenticated_client):
    """認証済みクライアントのセッション状態を確認"""
    client = authenticated_client

    # セッション内容を確認
    with client.session_transaction() as sess:
        print(f"Session contents: {dict(sess)}")
        assert "session_id" in sess
        print(f"Session ID: {sess['session_id']}")

    # インデックスページにアクセス
    response = client.get("/")
    print(f"Response status: {response.status_code}")
    print(
        f"Response location: {response.location if response.status_code in [301, 302] else 'N/A'}"
    )

    # AuthManagerでセッションを確認
    import sqlite3

    from src.auth import AuthManager
    from src.config import DB_PATH

    # データベースの内容を確認
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # sessionsテーブルの内容を確認
    cursor.execute("SELECT * FROM sessions")
    sessions = cursor.fetchall()
    print(f"Sessions in DB: {sessions}")

    # usersテーブルの内容を確認
    cursor.execute("SELECT id, username, email FROM users")
    users = cursor.fetchall()
    print(f"Users in DB: {users}")

    conn.close()

    with client.session_transaction() as sess:
        user = AuthManager.get_user_by_session(sess.get("session_id"))
        print(f"User found: {user.username if user else 'None'}")

    assert response.status_code == 200, f"Expected 200, got {response.status_code}"
