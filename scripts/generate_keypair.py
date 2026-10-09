from __future__ import annotations

from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

ROOT = Path(__file__).resolve().parents[1]
SECRET_DIR = ROOT / ".secrets"
PRIVATE_PATH = SECRET_DIR / "rsa_key.p8"
PUBLIC_PATH = SECRET_DIR / "rsa_key.pub"


def main() -> int:
    SECRET_DIR.mkdir(exist_ok=True)
    if PRIVATE_PATH.exists() or PUBLIC_PATH.exists():
        print("Key files already exist. Delete .secrets/rsa_key.* first if you intentionally want rotation.")
        return 1

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    PRIVATE_PATH.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    public = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    PUBLIC_PATH.write_bytes(public)

    public_body = "".join(
        line.strip()
        for line in public.decode().splitlines()
        if "BEGIN PUBLIC KEY" not in line and "END PUBLIC KEY" not in line
    )
    print(f"Created: {PRIVATE_PATH}")
    print(f"Created: {PUBLIC_PATH}")
    print("\nRun this once in Snowflake Snowsight with a role allowed to alter your user:")
    print(f"ALTER USER <YOUR_SNOWFLAKE_USER> SET RSA_PUBLIC_KEY='{public_body}';")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
