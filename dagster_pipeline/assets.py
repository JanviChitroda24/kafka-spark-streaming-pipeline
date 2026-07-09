# Author: Janvi Chitroda | github.com/JanviChitroda24
"""
Dagster assets for the streaming pipeline.

Asset graph (runs in order):
  check_infrastructure → produce_trades → process_stream
      → quality_checks → load_to_snowflake

Each asset wraps an existing script via subprocess — we reuse previous code,
not rewrite logic inside Dagster. Orchestration handles order, retries, and UI.

Run Dagster UI (from repo root — required for imports):
  dagster dev -f dagster_pipeline/definitions.py -d .
  → http://localhost:3000

The -d . flag sets working directory to repo root so
`from dagster_pipeline.assets import ...` resolves correctly.
"""

import os
import signal
import subprocess
import time

from dagster import AssetExecutionContext, Config, RetryPolicy, asset
from pydantic import Field

# Repo root — subprocess cwd so paths match manual runs (src/producer.py, etc.)
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


class ProducerConfig(Config):
    """Configurable producer settings — set in Dagster UI when materializing produce_trades."""

    mode: str = Field(default="simulated", description="simulated | real | auto")
    eps: int = Field(default=100, description="Events per second (simulated mode)")
    duration: int = Field(default=120, description="Run duration in seconds")


@asset(
    description="Verify Docker and Redpanda are running",
    retry_policy=RetryPolicy(max_retries=2, delay=5),
)
def check_infrastructure(context: AssetExecutionContext) -> dict:
    """
    Gate asset — nothing runs until Kafka/Redpanda is healthy.

    If Redpanda is down, attempts docker compose up -d and waits for startup.
    """
    result = subprocess.run(
        ["docker", "exec", "redpanda", "rpk", "cluster", "health"],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if result.returncode != 0:
        context.log.warning("Redpanda not healthy. Starting Docker Compose...")
        subprocess.run(
            ["docker", "compose", "up", "-d"],
            cwd=REPO_ROOT,
            timeout=60,
            check=False,
        )
        time.sleep(15)

    topic_result = subprocess.run(
        ["docker", "exec", "redpanda", "rpk", "topic", "list"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert "stock_trades" in topic_result.stdout, "Topic stock_trades not found!"
    context.log.info("Infrastructure healthy. Redpanda running, topic exists.")
    return {"status": "healthy"}


@asset(
    deps=[check_infrastructure],
    description="Run the trade producer for configured duration",
    retry_policy=RetryPolicy(max_retries=1, delay=10),
)
def produce_trades(context: AssetExecutionContext, config: ProducerConfig) -> dict:
    """Publish trades to Kafka — simulated, Finnhub, or auto-detect mode."""
    context.log.info(
        f"Starting producer: mode={config.mode}, eps={config.eps}, duration={config.duration}"
    )
    result = subprocess.run(
        [
            "python",
            "src/producer.py",
            "--mode",
            config.mode,
            "--eps",
            str(config.eps),
            "--duration",
            str(config.duration),
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=config.duration + 30,
        check=False,
    )
    if result.stdout:
        context.log.info(f"Producer output: {result.stdout[-500:]}")
    if result.returncode != 0:
        context.log.error(f"Producer error: {result.stderr[-500:]}")
        raise RuntimeError("Producer failed")
    return {"mode": config.mode, "duration": config.duration}


@asset(
    deps=[produce_trades],
    description="Run Spark streaming processor",
    retry_policy=RetryPolicy(max_retries=2, delay=15),
)
def process_stream(context: AssetExecutionContext) -> dict:
    """
    Run stream_processor.py for a fixed window, then SIGINT to stop gracefully.

    Structured Streaming is long-running; for orchestrated demo runs we process
    buffered Kafka data for ~90s then stop. Production would use a separate
    always-on deployment or Dagster sensor on Kafka lag.
    """
    context.log.info("Starting Spark Structured Streaming processor...")
    proc = subprocess.Popen(
        ["python", "src/stream_processor.py"],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    context.log.info("Waiting 90 seconds for Spark to process...")
    time.sleep(90)
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
    context.log.info("Spark processor completed.")
    return {"status": "processed"}


@asset(
    deps=[process_stream],
    description="Run 7 data quality checks on Delta output",
)
def quality_checks(context: AssetExecutionContext) -> dict:
    """
    DQ gate — Snowflake load only runs if all 7 checks pass.

    data_quality_checks.py exits 1 on failure; we also scan stdout for FAILED.
    """
    result = subprocess.run(
        ["python", "src/data_quality_checks.py"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=120,
        check=False,
    )
    if result.stdout:
        context.log.info(result.stdout)
    if result.returncode != 0 or "FAILED" in result.stdout:
        raise RuntimeError("Data quality checks failed! See logs above.")
    return {"status": "all_passed"}


@asset(
    deps=[quality_checks],
    description="Load verified VWAP results to Snowflake",
    retry_policy=RetryPolicy(max_retries=2, delay=10),
)
def load_to_snowflake(context: AssetExecutionContext) -> dict:
    """Load VWAP tables to Snowflake — only after quality_checks passes."""
    result = subprocess.run(
        ["python", "src/snowflake_loader.py"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=180,
        check=False,
    )
    if result.stdout:
        context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(f"Snowflake loader error: {result.stderr[-500:]}")
        raise RuntimeError("Snowflake load failed")
    return {"status": "loaded"}
