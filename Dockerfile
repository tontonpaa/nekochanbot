# ベースイメージとしてPython 3.12-slimを選択
FROM python:3.12-slim-bookworm

# 作業ディレクトリを設定
WORKDIR /app

# pipをアップグレードし、requirements.txtからPythonの依存関係をインストール
# --no-cache-dir を使用してイメージサイズを削減
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt --no-cache-dir

# アプリケーションのソースコードをコンテナにコピー
# このDockerfileと同じディレクトリにある全てのファイルとフォルダがコピーされます。
# .dockerignoreファイルを使用して、不要なファイル（例: .git, .venvなど）を除外することを推奨します。
COPY . .

# ボットのメインファイルが "nekochanbot.py" であることを想定しています。
# ファイル名が異なる場合は、適宜修正してください。
CMD ["python3", "nekochanbot.py"]
