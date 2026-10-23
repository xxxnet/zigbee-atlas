FROM ghcr.io/home-assistant/base:latest
RUN apk add --no-cache python3 py3-yaml graphviz tzdata
ENV PYTHONUNBUFFERED=1 ATLAS_INGRESS=true
COPY app /app
WORKDIR /app
CMD ["python3", "/app/app.py"]
