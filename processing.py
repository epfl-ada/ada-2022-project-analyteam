"""
    This module contains functions and classes for data processing and transformation
"""

import re

__USA_REGEX = r".*(USA).*|.*(United States)(of America)?.*"
__NAN_REGEX = r"[ ]*[nN][a][nN][ ]*"

def get_state(location: str):
    if location is None:
        return None
    
    if re.match(pattern=__USA_REGEX, string=location):
        return location.split(sep=",")[1].strip()

def get_country(location: str):
    """_summary_

    Args:
        location (str): _description_

    Returns:
        _type_: _description_
    """
    if location is None or re.match(pattern=__NAN_REGEX, string=location):
        return None
    
    if re.match(pattern=__USA_REGEX, string=location):
        return "United States"
    
    return location

def str_nan_to_none(text: str):
    """_summary_

    Args:
        text (str): _description_

    Returns:
        _type_: _description_
    """
    if text is None:
        return None
    
    if re.match(pattern=__NAN_REGEX, string=text):
        return None
    
    return text