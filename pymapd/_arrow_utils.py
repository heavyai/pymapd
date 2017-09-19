def is_null_type(t):
    return t.id == 0


def is_bool_type(t):
    return t.id == 1


def is_int_type(t):
    """Whether a type is an integer type

    This matches for (u)int8, (u)int16, (u)int32, (u)int64
    """
    return t.id in {2, 3, 4, 5, 6, 7, 8, 9}


def is_float_type(t):
    """Whether a types is a floating type

    One of float16, float32, or float64
    """
    return t.id in {10, 11, 12}


def is_time_type(t):
    return t.id in {19, 20}


def is_decimal_type(t):
    return t.id == 22


def is_datetime_type(t):
    return t.id == 18


def is_date_type(t):
    return t.id in {16, 17}


def is_bytes_type(t):
    return t.id == 14


def is_string_type(t):
    return t.id == 13
