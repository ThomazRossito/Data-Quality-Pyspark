from datetime import datetime

from pytz import timezone

sao_paulo_timezone = timezone("America/Sao_Paulo")


class Colorize:
    """
    Class used to colorize logger prints.
    """

    styles = {
        "default": 0,  # Turn off all attributes
        "bold": 1,  # Set bold mode
        "unable": 2,  # Set unable mode
        "italic": 3,  # Set italic mode
        "underline": 4,  # Set underline mode
        "blink": 5,  # Set blink mode
        "swap": 7,  # Exchange foreground and background colors
        # Hide text (foreground color would be the same as background)
        "hide": 8,
        "strykethrough": 9,  # Exchange foreground and background colors
    }

    colors = {
        "black": 30,
        "red": 31,
        "green": 32,
        "yellow": 33,
        "blue": 34,
        "magenta": 35,
        "cyan": 36,
        "white": 37,
        "default": 39,
        "on_black": 40,
        "on_red": 41,
        "on_green": 42,
        "on_yellow": 43,
        "on_blue": 44,
        "on_magenta": 45,
        "on_cyan": 46,
        "on_white": 47,
    }

    @classmethod
    def get_color(cls, text: str, color="default", style="default"):
        """
        Returns the text with the given style

        :param text: Text that will be written
        :type text: str
        :param color: Color of text, defaults to 'default'
        :type color: str, optional
        :param style: Style of text, defaults to 'default'
        :type style: str, optional
        :return: Text with styles and colors applied
        :rtype: str
        """
        return f"\x1b[{cls.styles[style]};{cls.colors[color]}m{text}\x1b[0m"


class Logger:
    """
    Logger class

    print only logs with criticity_level equal or less level informed
    """

    current_level = 5
    levels = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]

    def __init__(self, level="NOTSET"):
        self.set_level(level)

    def set_level(self, level):
        try:
            self.current_level = self.levels.index(level)
        except ValueError:
            self.error(
                f"{level} is not a accepted level. Try one of this: {self.levels}"
            )

    @staticmethod
    def _execute(msg):

        full_msg = f'[{datetime.now().astimezone(sao_paulo_timezone).strftime("%m/%d %H:%M:%S")}] - {msg}'
        print(full_msg)

        return full_msg

    def debug(self, msg):
        if self.levels.index("DEBUG") <= self.current_level:
            return self._execute(
                f"{Colorize.get_color('[Debug]: ', color='green', style='bold')} {msg}"
            )

    def info(self, msg):
        if self.levels.index("INFO") <= self.current_level:
            return self._execute(
                f"{Colorize.get_color('[Info]: ', color='cyan', style='bold')} {msg}"
            )

    def warn(self, msg):
        if self.levels.index("WARNING") <= self.current_level:
            return self._execute(
                f"{Colorize.get_color('[Warning]: ', color='yellow', style='bold')} {msg}"
            )

    def error(self, msg):
        if self.levels.index("ERROR") <= self.current_level:
            return self._execute(
                f"{Colorize.get_color('[Error]: ', color='red', style='blink')} {msg}"
            )

    def critical(self, msg):
        if self.levels.index("CRITICAL") <= self.current_level:
            return self._execute(
                f"{Colorize.get_color('[CRITICAL]: ', color='on_red', style='blink')} {msg}"
            )


logger = Logger(level="INFO")

__all__ = ["logger"]
