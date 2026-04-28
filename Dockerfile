# ── Builder stage ─────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Non-root user
RUN useradd -m -u 1000 botuser

# Copy installed packages from builder
COPY --from=builder /root/.local /home/botuser/.local

# Copy application code
COPY --chown=botuser:botuser . .

# Create required directories
RUN mkdir -p logs data && chown -R botuser:botuser logs data

USER botuser

ENV PYTHONPATH=/app
ENV PATH=/home/botuser/.local/bin:$PATH

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:${DASHBOARD_PORT:-8080}/api/pnl || exit 1

EXPOSE 8080

CMD ["python", "-m", "bot.engine"]
