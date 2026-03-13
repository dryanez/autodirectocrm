import sqlite3, shutil, tempfile, hashlib, subprocess, sys
from pathlib import Path
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.backends import default_backend

key_proc = subprocess.run(['security', 'find-generic-password', '-s', 'Chrome Safe Storage', '-w'],
    capture_output=True, text=True, timeout=10)
safe_storage_key = key_proc.stdout.strip()
print(f'Key len: {len(safe_storage_key)}, returncode: {key_proc.returncode}', flush=True)

derived_key = hashlib.pbkdf2_hmac('sha1', safe_storage_key.encode('utf-8'), b'saltysalt', 1003, dklen=16)
print(f'Derived key hex: {derived_key.hex()}', flush=True)

db = Path.home() / 'Library/Application Support/Google/Chrome/Default/Cookies'
tmp = Path(tempfile.mktemp(suffix='.db'))
shutil.copy2(str(db), str(tmp))
conn = sqlite3.connect(str(tmp))
rows = conn.execute("SELECT name, encrypted_value FROM cookies WHERE host_key LIKE '%facebook.com'").fetchall()
conn.close()
tmp.unlink()

print(f'Total FB cookies: {len(rows)}', flush=True)
print(f'Cookie names: {[r[0] for r in rows]}', flush=True)

# Try each cookie with CBC + spaces IV and check if the tail looks valid
iv = b' ' * 16
good_cookies = []
for name, enc in rows:
    if not enc or enc[:3] != b'v10':
        continue
    ct = enc[3:]
    try:
        cipher = Cipher(algorithms.AES(derived_key), modes.CBC(iv), backend=default_backend())
        dec = cipher.decryptor()
        pt = dec.update(ct) + dec.finalize()
        pad = pt[-1] if pt else 0
        if isinstance(pad, int) and 0 < pad <= 16:
            pt = pt[:-pad]
        # The first block (16 bytes) is garbage due to wrong IV.
        # Skip it and use the rest if it's valid ASCII
        if len(pt) > 16:
            tail = pt[16:]
            try:
                text = tail.decode('ascii')
                good_cookies.append((name, text))
                print(f'  {name}: (skipped 16B) => {repr(text[:60])}', flush=True)
            except UnicodeDecodeError:
                text = pt.decode('utf-8', errors='replace')
                print(f'  {name}: INVALID even after skip16 => {repr(text[:60])}', flush=True)
        else:
            # Short cookie (1 block only) — entire value is garbage
            text = pt.decode('utf-8', errors='replace')
            print(f'  {name}: SHORT ({len(pt)}B) => {repr(text[:60])}', flush=True)
    except Exception as e:
        print(f'  {name}: ERROR: {e}', flush=True)

for name, enc in rows:
    if not enc:
        continue
    prefix = enc[:3]
    print(f'\nCookie: {name}, len={len(enc)}, first 6 bytes hex: {enc[:6].hex()}', flush=True)

    if prefix == b'v10':
        # Method 1: CBC with spaces IV (classic Chrome macOS)
        iv1 = b' ' * 16
        ct1 = enc[3:]
        try:
            cipher = Cipher(algorithms.AES(derived_key), modes.CBC(iv1), backend=default_backend())
            dec = cipher.decryptor()
            pt = dec.update(ct1) + dec.finalize()
            pad = pt[-1] if pt else 0
            if isinstance(pad, int) and 0 < pad <= 16:
                pt = pt[:-pad]
            text = pt.decode('utf-8', errors='replace')
            is_ascii = all(32 <= ord(c) < 127 for c in text)
            print(f'  [CBC spaces-IV] {repr(text[:60])} ascii={is_ascii}', flush=True)
        except Exception as e:
            print(f'  [CBC spaces-IV] Error: {e}', flush=True)

        # Method 2: CBC with the first 16 bytes after v10 being the IV
        if len(enc) > 3 + 16:
            iv2 = enc[3:3+16]
            ct2 = enc[3+16:]
            if len(ct2) % 16 == 0 and len(ct2) > 0:
                try:
                    cipher2 = Cipher(algorithms.AES(derived_key), modes.CBC(iv2), backend=default_backend())
                    dec2 = cipher2.decryptor()
                    pt2 = dec2.update(ct2) + dec2.finalize()
                    pad2 = pt2[-1] if pt2 else 0
                    if isinstance(pad2, int) and 0 < pad2 <= 16:
                        pt2 = pt2[:-pad2]
                    text2 = pt2.decode('utf-8', errors='replace')
                    is_ascii2 = all(32 <= ord(c) < 127 for c in text2)
                    print(f'  [CBC data-IV] {repr(text2[:60])} ascii={is_ascii2}', flush=True)
                except Exception as e:
                    print(f'  [CBC data-IV] Error: {e}', flush=True)

        # Method 3: GCM with nonce = enc[3:3+12], ct = enc[3+12:-16], tag = enc[-16:]
        if len(enc) > 3 + 12 + 16:
            nonce3 = enc[3:3+12]
            ct3 = enc[3+12:]  # includes the auth tag at the end
            try:
                aesgcm = AESGCM(derived_key)
                pt3 = aesgcm.decrypt(nonce3, ct3, None)
                text3 = pt3.decode('utf-8', errors='replace')
                is_ascii3 = all(32 <= ord(c) < 127 for c in text3)
                print(f'  [GCM] {repr(text3[:60])} ascii={is_ascii3}', flush=True)
            except Exception as e:
                print(f'  [GCM] Error: {e}', flush=True)
    else:
        print(f'  Unknown prefix: {prefix}', flush=True)

print('\nDONE', flush=True)
