from libtrafficbot import sec


def is_xpath_locator(locator_string: str) -> bool:
    """
    Checks if locator string is XPath Element
    """
    if locator_string.startswith("/"):
        return True
    return False


def calculate_chance_weight_based(data: [tuple]):
    result = []
    for x in data:
        for r in range(int(x[1] * 100)):
            result.append(x[0])
    return sec.choice(result)


def calculate_bool_percentage_based(data: float or int) -> bool:
    if (500 - data * 10 / 2) < sec.randint(0, 1000) < (500 + data * 10 / 2):
        return True
    return False


def calculate_chance_percentage_based(data) -> bool:
    result = []
    for x in data:
        for r in range(int(x[1] * 10)):
            result.append(x[0])
    return sec.choice(result)


def choose_fair(data):
    if isinstance(data, float) or isinstance(data, int):
        return calculate_bool_percentage_based(data)
    elif (isinstance(data, tuple) or isinstance(data, list)) and (isinstance(data[0], tuple) or isinstance(data[0], list)):
        return calculate_chance_weight_based(data)
    elif (isinstance(data, tuple) or isinstance(data, list)) and (not (isinstance(data[0], tuple) or isinstance(data[0], list))):
        return sec.choice(data)
    elif isinstance(data, str) and isinstance(float(data), float):
        return calculate_bool_percentage_based(data)
