import re
import traceback

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


def get_last_file_and_next_line(traceback_part):
    # Define the regex pattern to match lines starting with "File"
    pattern = r'(File .*?)\n(.*?)(?=\nFile|$)'

    # Find all occurrences of lines starting with "File" in the traceback_part
    matches = re.findall(pattern, traceback_part, re.MULTILINE | re.DOTALL)

    if matches:
        # Get the last occurrence
        last_file_line, next_line = matches[-1]
        return last_file_line.strip(), next_line.strip()
    else:
        return None, None


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
        # Extract variables from kwargs
        traceback_part = traceback.format_exc(limit=4)  # Limit specifies how many frames to capture
        last_file_line, next_line = get_last_file_and_next_line(traceback_part)
        console.print("------------", style='red')
        console.print(last_file_line, style='red')
        console.print(f"{message}", style='red')
        console.print("Corrupt Code:", next_line, style='red')
        console.print("Variables")
        console.print(context.print_all_data_references(), style='red')
        console.print("------------", style='red')
