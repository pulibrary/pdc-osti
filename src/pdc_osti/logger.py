import logging
from datetime import datetime
from functools import lru_cache
from logging.config import dictConfig
from pathlib import Path

import yaml
from rich.logging import Console, RichHandler

from . import LOG_NAME
from . import __version__ as pdc_osti_version

LOG_CONF = Path("configs/log_conf.yml")


@lru_cache()
def log_configure() -> logging.Logger:
    """Configure stdout logging"""

    with open(LOG_CONF, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    dictConfig(config)
    stream_log = logging.getLogger(LOG_NAME)
    return stream_log


def file_logging(prefix: str) -> [logging.Logger, Path]:
    """Configure file logging"""

    now = datetime.now()
    log_file = Path(f"logs/{prefix}.log-{now:%Y-%m-%d}")
    if not (parent := log_file.parent).exists():  # pragma: no cover
        parent.mkdir(parents=True)

    console = Console(
        force_terminal=False,
        file=open(log_file, "a"),
        width=120,
        color_system="truecolor",
    )
    fh = RichHandler(
        console=console,
        level=logging.DEBUG,
        log_time_format="[%X]",
        markup=True,
        rich_tracebacks=True,
        tracebacks_show_locals=True,
        enable_link_path=False,
    )
    pdc_log.addHandler(fh)
    return pdc_log, log_file


def script_log_init(prefix: str) -> logging.Logger:
    """Script log initialization"""
    log, log_file = file_logging(prefix)
    log.info(f"[bold yellow]Starting {prefix}")
    log.info(f"File logging: {log_file}")
    log.info(f"Version: {pdc_osti_version}")
    return log


def script_log_end(prefix: str, log: logging.Logger):
    """Script log completion"""
    log.info(f"[bold dark_green]✔ Completed {prefix}!")
    log.debug("")


pdc_log = log_configure()
