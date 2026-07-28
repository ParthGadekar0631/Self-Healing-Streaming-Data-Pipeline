"""Unified command entry point for the pipeline, API, and producer."""

from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description="Self-Healing Streaming Data Pipeline")
    parser.add_argument("service", choices=("pipeline", "api", "producer"))
    args, remainder = parser.parse_known_args()
    if args.service == "pipeline":
        from src.streaming.pipeline import main as run
    elif args.service == "api":
        from src.api.main import main as run
    else:
        from src.producer.kafka_producer import main as run
    run()


if __name__ == "__main__":
    main()
