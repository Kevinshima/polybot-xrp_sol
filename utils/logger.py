"""Structured logging via loguru."""
from pathlib import Path
from loguru import logger
from config import settings


def _normalized_level_name(level_name: str) -> str:
    try:
        return logger.level(str(level_name).upper()).name
    except ValueError:
        return "INFO"


def _base_log_level_name() -> str:
    return _normalized_level_name(settings.LOG_LEVEL)


def _min_level_name_for_record(record) -> str:
    return _base_log_level_name()


def _profile_filter(record) -> bool:
    min_level_name = _min_level_name_for_record(record)
    return record["level"].no >= logger.level(min_level_name).no


def setup_logger():
    logger.remove()

    # Plain text to log file
    log_path = Path(settings.LOG_FILE)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(log_path),
        level="DEBUG",
        filter=_profile_filter,
        rotation="100 MB",
        retention="7 days",
        compression="gz",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {module}:{function}:{line} | {message}",
    )

    return logger


setup_logger()

__all__ = ["logger"]
