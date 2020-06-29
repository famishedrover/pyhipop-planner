def negate(function):
    def new_function(*args, **kwargs):
        return not function(*args, **kwargs)
    return new_function
