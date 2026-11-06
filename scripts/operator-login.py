#!/usr/bin/env python3
"""Operator helper for the fulfillment adapter: log in to Keycloak realm box1 and call the protected API.

Logs in as client fulfillment-adapter with Authorization Code + PKCE through your browser (loopback redirect),
then makes one call. The access token is held in memory only: never printed, logged or written to disk.
Requires the client role fulfillment-writer. Python 3 standard library only.

  scripts/operator-login.py create request.json         # POST /api/fulfillments (file with the request body; - for stdin)
  scripts/operator-login.py reconcile ful_1042          # POST /internal/fulfillments/{id}/reconcile
  scripts/operator-login.py reconcile-all UPLOAD_EXHAUSTED --limit 200
  scripts/operator-login.py status ful_1042             # GET (no login when the read policy is permit)

Defaults target the dev environment; override with --api and --realm.
"""
import argparse
import base64
import hashlib
import http.server
import json
import secrets
import sys
import urllib.error
import urllib.parse
import urllib.request
import webbrowser

DEFAULT_API = "https://fulfillment-adapter-dev.apps.ocp.box1.org"
DEFAULT_REALM = "https://keycloak.apps.ocp.box1.org/auth/realms/box1"
CLIENT_ID = "fulfillment-adapter"


def login(realm: str) -> str:
    """Browser login with PKCE; returns the access token."""
    endpoint = realm.rstrip("/") + "/protocol/openid-connect"
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    state = secrets.token_urlsafe(16)
    result = {}

    class Callback(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 (http.server naming)
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            result.update(code=query.get("code", [None])[0], state=query.get("state", [None])[0],
                          error=query.get("error", [None])[0])
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Login complete; return to the terminal.")

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Callback)
    redirect = f"http://127.0.0.1:{server.server_port}/callback"
    url = endpoint + "/auth?" + urllib.parse.urlencode({
        "client_id": CLIENT_ID, "response_type": "code", "scope": "openid", "redirect_uri": redirect,
        "state": state, "code_challenge": challenge, "code_challenge_method": "S256"})
    print("Opening the browser for the box1 login. If it does not open, visit:\n" + url, file=sys.stderr)
    webbrowser.open(url)
    server.timeout = 300
    server.handle_request()
    server.server_close()
    if result.get("error") or result.get("state") != state or not result.get("code"):
        sys.exit("Login failed: " + str(result.get("error") or "no code or state mismatch"))
    body = urllib.parse.urlencode({"grant_type": "authorization_code", "client_id": CLIENT_ID, "code": result["code"],
                                   "redirect_uri": redirect, "code_verifier": verifier}).encode()
    with urllib.request.urlopen(urllib.request.Request(endpoint + "/token", data=body), timeout=30) as response:
        return json.load(response)["access_token"]


def call(method: str, url: str, token=None, body=None):
    headers = {"Accept": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status, location, text = response.status, response.headers.get("Location"), response.read().decode()
    except urllib.error.HTTPError as error:
        status, location, text = error.code, None, error.read().decode()
    print(status, location or "")
    try:
        print(json.dumps(json.loads(text), indent=2))
    except ValueError:
        print(text)
    return 0 if status < 400 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api", default=DEFAULT_API, help="adapter base URL (default: dev Route)")
    parser.add_argument("--realm", default=DEFAULT_REALM, help="Keycloak realm URL (default: box1)")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create", help="accept a fulfillment request (JSON body from a file, or - for stdin)")
    create.add_argument("file")
    reconcile = sub.add_parser("reconcile", help="requeue one NEEDS_ATTENTION request")
    reconcile.add_argument("fulfillment_request_id")
    bulk = sub.add_parser("reconcile-all", help="requeue NEEDS_ATTENTION requests with one transport reason")
    bulk.add_argument("reason", choices=["NO_ACKNOWLEDGEMENT", "UPLOAD_EXHAUSTED", "ERR_RESENDS_EXHAUSTED"])
    bulk.add_argument("--limit", type=int, default=100)
    status = sub.add_parser("status", help="read a request's status")
    status.add_argument("fulfillment_request_id")
    status.add_argument("--login", action="store_true", help="log in first (when the read policy is authenticated)")
    args = parser.parse_args()
    api = args.api.rstrip("/")
    quote = lambda value: urllib.parse.quote(value, safe="")  # noqa: E731

    if args.command == "status":
        token = login(args.realm) if args.login else None
        return call("GET", f"{api}/api/fulfillments/{quote(args.fulfillment_request_id)}", token)
    if args.command == "create":
        with (sys.stdin if args.file == "-" else open(args.file, encoding="utf-8")) as source:
            body = json.load(source)
        return call("POST", f"{api}/api/fulfillments", login(args.realm), body)
    if args.command == "reconcile":
        return call("POST", f"{api}/internal/fulfillments/{quote(args.fulfillment_request_id)}/reconcile",
                    login(args.realm))
    query = urllib.parse.urlencode({"reason": args.reason, "limit": args.limit})
    return call("POST", f"{api}/internal/fulfillments/reconcile?{query}", login(args.realm))


if __name__ == "__main__":
    sys.exit(main())
