import logging
from datetime import datetime


class MessagingLogger(logging.Logger):
    """
    An implementation of Logger that lets the user direct output to stdout, if desired.
    Also adds some timestamps and other info.
    """

    def __init__(self, name, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self._lowest_print_level = logging.WARNING
        self._print_to_stdout = False

    def set_print_to_stdout(self, val: bool):
        """If True, output will go to stdout"""
        self._print_to_stdout = val

    def log(self, level, msg, *args, **kwargs):
        """Override of standard function"""
        super().log(level, msg, *args, **kwargs)

    def setLevel(self, level):
        """Override of standard function"""
        super().setLevel(level)
        self._lowest_print_level = level

    @staticmethod
    def make_instance(name, level, print_to_stdout):
        """
        Creates an instance of MessageLogger that's immediately usable.
        :param name: name of file
        :param level: level to pass to setLevel()
        :param print_to_stdout: if True, any messages of appropriate level will be printed
            to stdout.
        :return: logger instance
        """
        logging.setLoggerClass(MessagingLogger)
        logger = logging.getLogger(name)
        logger.setLevel(level=level)
        logger.set_print_to_stdout(print_to_stdout)
        logging.setLoggerClass(logging.Logger)
        return logger

    def _log(
        self,
        level,
        msg,
        args,
        exc_info=None,
        extra=None,
        stack_info=False,
        stacklevel=1,
    ):
        """Override of standard function"""
        if level >= self._lowest_print_level and self._print_to_stdout:
            message_name = "DEBUG"
            if level >= logging.ERROR:
                message_name = "ERROR"
            elif level >= logging.WARNING:
                message_name = "WARNING"
            elif level >= logging.INFO:
                message_name = "INFO"
            timestamp = datetime.now().isoformat(sep=" ")
            print(f"dispatchio {message_name} at {timestamp}: {msg}")
        super()._log(
            level,
            msg,
            args,
            exc_info=exc_info,
            extra=extra,
            stack_info=stack_info,
            stacklevel=stacklevel,
        )
