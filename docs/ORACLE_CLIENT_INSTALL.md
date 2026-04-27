# Oracle Client Install

`oracledb` bisa berjalan dalam thin mode tanpa Oracle Client. Namun beberapa database lama membutuhkan thick mode, misalnya saat muncul error:

```text
DPY-3015: password verifier type ... is not supported by python-oracledb in thin mode
```

Untuk kasus itu, install Oracle Instant Client dan set `ORACLE_CLIENT_LIB_DIR`.

## Linux Install

Install dependency OS:

```bash
sudo apt-get update
sudo apt-get install -y unzip libaio1 libnsl2
```

Download Oracle Instant Client Basic 23.9 untuk Linux x64 dari halaman resmi Oracle, lalu letakkan zip di `/tmp`.

Contoh nama file:

```text
instantclient-basic-linux.x64-23.9.0.25.07.zip
```

Extract ke `/opt/oracle`:

```bash
sudo mkdir -p /opt/oracle
sudo unzip -o /tmp/instantclient-basic-linux.x64-23.9.0.25.07.zip -d /opt/oracle
sudo ln -sfn /opt/oracle/instantclient_23_9 /opt/oracle/instantclient
```

Daftarkan library ke dynamic linker:

```bash
echo /opt/oracle/instantclient_23_9 | sudo tee /etc/ld.so.conf.d/oracle-instantclient.conf
sudo ldconfig
```

Isi `.env`:

```dotenv
ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_23_9
```

Test thick mode:

```bash
source .venv/bin/activate
python - <<'PY'
import oracledb
oracledb.init_oracle_client(lib_dir="/opt/oracle/instantclient_23_9")
print("thin mode:", oracledb.is_thin_mode())
PY
```

Output yang benar untuk thick mode:

```text
thin mode: False
```

## Tanpa ldconfig

Jika tidak bisa menjalankan `sudo ldconfig`, jalankan command dengan `LD_LIBRARY_PATH`:

```bash
LD_LIBRARY_PATH=/opt/oracle/instantclient_23_9 \
python -m oracle_pg_sync sync --config config.yaml --direction oracle-to-postgres --tables public.sample_customer
```

## Common Error

`DPI-1047: Cannot locate a 64-bit Oracle Client library`

- `ORACLE_CLIENT_LIB_DIR` salah.
- File Instant Client belum diextract.
- Dynamic linker belum tahu lokasi library.
- Gunakan `ldconfig` atau prefix `LD_LIBRARY_PATH`.

`libnnz.so: cannot open shared object file`

- Library dependency tidak ditemukan oleh dynamic linker.
- Jalankan `sudo ldconfig` setelah menambahkan `/etc/ld.so.conf.d/oracle-instantclient.conf`.
- Atau gunakan `LD_LIBRARY_PATH=/opt/oracle/instantclient_23_9`.
