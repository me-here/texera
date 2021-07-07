import re
from betterproto import which_one_of, Message

pattern = re.compile(r'(?<!^)(?=[A-Z])')


def get_oneof(base: Message) -> Message:
    _, value = which_one_of(base, "sealed_value")
    return value


def set_oneof(base: type(Message), value: Message) -> Message:
    snake_case_name = re.sub(pattern, '_', value.__class__.__name__).lower()
    ret = base()
    ret.__setattr__(snake_case_name, value)
    return ret


Message.__hash__ = lambda x: hash(x.__repr__())
