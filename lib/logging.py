import logging


def get_logger() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(process)d - %(name)s - " "%(levelname)s - %(message)s",
    )
    return logging.getLogger("gmail-size")


def format_number(number: int) -> str:
    metric = "B"
    if number > 5120:
        number = int(number / 1024)
        metric = "KB"
    if number > 5120:
        number = int(number / 1024)
        metric = "MB"
    if number > 5120:
        number = int(number / 1024)
        metric = "GB"

    return f"{number} {metric}"
