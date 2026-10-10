# Vytel's Academy Chronicles game server (relay + launcher version.json). Pure Python standard library.
# Works on Render (Docker), Koyeb, Fly.io, Railway, an Oracle/Google VM with Docker, or locally:
#   docker build -t vytel-server . && docker run -p 8000:8000 -e VERSION_URL=... vytel-server
FROM python:3.12-slim
WORKDIR /app
RUN useradd --create-home --uid 10001 vytel
COPY server_v2.py .
# Optional: a builds/ folder with version.json (and small files) can be baked in. Game zips stay OUT of the image:
# they are downloaded from GitHub Releases (see .dockerignore and USER_STEPS.md).
ENV PYTHONUNBUFFERED=1 \
    PORT=8000 \
    SERVE_WEB=0
USER vytel
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=4s CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/healthz' % os.environ.get('PORT','8000'), timeout=3)"
# PORT, PUBLIC_URL, VERSION_URL, MAX_ROOMS, LOBBY_ALL ... are read from the environment (see server_v2.py --help).
CMD ["python", "server_v2.py"]
