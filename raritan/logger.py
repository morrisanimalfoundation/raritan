
from rich.console import Console

from raritan.context import context

"""
Some very simple rich enabled CLI output helpers.
"""

# A logger instance for use here and elsewhere.
console = Console()


def info(message, **kwargs) -> None:
    """
    Logs an informational message.

    Parameters
    ----------
    message: str
      The output.
    kwargs: dict
      Any kwargs to pass to the console.
    """
    if not context.no_logging:
        console.print(message, style='blue', **kwargs)


def success(message, **kwargs) -> None:
    """
    Logs a success message.

    Parameters
    ----------
    message: str
      The output.
    kwargs: dict
      Any kwargs to pass to the console.
    """
    if not context.no_logging:
        console.print(message, style='green', **kwargs)


def warning(message, **kwargs) -> None:
    """
    Logs a warning message.

    Parameters
    ----------
    message: str
      The output.
    kwargs: dict
      Any kwargs to pass to the console.
    """
    if not context.no_logging:
        console.print(message, style='yellow', **kwargs)


def error(message, **kwargs) -> None:
    """
    Logs an error message.

    Parameters
    ----------
    message: str
      The output.
    kwargs: dict
      Any kwargs to pass to the console.
    """
    if not context.no_logging:
        console.print(message, style='red', **kwargs)
