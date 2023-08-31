import logging

from rich.console import Console
from rich.logging import RichHandler
from rich.theme import Theme

from raritan.context import context

"""
Provides logging utilities.
"""

# We are generally only ever going to be operating in a logging or no logging state.
# Store and make static, a configured logger for our current run mode.
active_logger = None


def add_logging_level(level_name: str, level_num: int, method_name=None) -> None:
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    level_name: str
        The name of the new logging level, visible in the console.
    level_num: int
        The numerical weight of the logging level.
    method_name: str
        The name of the method to attach to the logger class, log.x().

    Example
    -------
    >>> add_logging_level('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
    5

    """
    if not method_name:
        method_name = level_name.lower()

    if hasattr(logging, level_name):
        raise AttributeError('{} already defined in logging module'.format(level_name))
    if hasattr(logging, method_name):
        raise AttributeError('{} already defined in logging module'.format(method_name))
    if hasattr(logging.getLoggerClass(), method_name):
        raise AttributeError('{} already defined in logger class'.format(method_name))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def log_for_level(self, message, *args, **kwargs):
        if self.isEnabledFor(level_num):
            self._log(level_num, message, args, **kwargs)

    def log_to_root(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)

    logging.addLevelName(level_num, level_name)
    setattr(logging, level_name, level_num)
    setattr(logging.getLoggerClass(), method_name, log_for_level)
    setattr(logging, method_name, log_to_root)


def get_logger():
    """
    Gets the configured logger for the current context.

    Notes
    -----
    Responds to the context.no_logging flag to produce a fully featured or silent logger.
    Only instantiates a logger once per execution.

    Returns
    -------
    active_logger: logging.Logger
        The configured logger instance.
    """
    global active_logger
    if active_logger is not None:
        return active_logger
    # Add our custom success level.
    # Used as logger.success().
    add_logging_level('SUCCESS', logging.DEBUG - 5)
    if not context.no_logging:
        # Make success loggings have a green flag.
        console = Console(theme=Theme({'logging.level.success': 'green'}))
        # Add a general formatting template.
        log_format = '%(message)s'
        # Apply basic configuration including the rich handler.
        logging.basicConfig(
            level='NOTSET', format=log_format, datefmt='[%X]', handlers=[RichHandler(console=console)]
        )
        # Stash our configured logger.
        active_logger = logging.getLogger('rich')
    else:
        # Set up a null logger for cases where we don't want logs output anywhere.
        active_logger = logging.getLogger('etl')
        active_logger.addHandler(logging.NullHandler())
    return active_logger
