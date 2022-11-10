"""
    This module contains functions and classes for data processing and transformation
"""

def country_of(location: str):
    """_summary_

    Args:
        location (str): _description_

    Returns:
        _type_: _description_
    """
    
    usa_regex = r".*(USA){1}.*|.*(United States){1}.*"
    nan_regex = r"[ ]*[nN][a][nN][ ]*"
    
    if location.match(usa_regex):
        return "USA"
    elif location.match(nan_regex):
        return ""
    else:
        return location
    