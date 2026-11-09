"""Mint a demo HS256 JWT for AUTH_MODE=jwt.

    JWT_SECRET=... python scripts/generate_token.py --tenant tenant-a --scope "search:read documents:read"

In production tokens come from the identity provider (OIDC client-credentials or user login);
this script only exists to exercise the API locally.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import jwt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--subject", default="demo-user")
    parser.add_argument("--scope", default="documents:read documents:write search:read")
    parser.add_argument("--audience", default="document-search")
    parser.add_argument("--ttl", type=int, default=3600, help="seconds until expiry")
    args = parser.parse_args()

    secret = os.environ.get("JWT_SECRET")
    if not secret or len(secret) < 32:
        sys.exit("Set JWT_SECRET (>= 32 chars) to the same value the API uses.")
    now = int(time.time())
    claims = {
        "sub": args.subject,
        "tenant_id": args.tenant,
        "scope": args.scope,
        "aud": args.audience,
        "iat": now,
        "exp": now + args.ttl,
    }
    print(jwt.encode(claims, secret, algorithm="HS256"))


if __name__ == "__main__":
    main()
