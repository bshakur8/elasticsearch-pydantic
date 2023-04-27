__all__ = ['classproperty']


class ClassPropertyDescriptor(property):
    def __get__(self, obj, klass=None):
        klass = klass or type(obj)
        return super().__get__(klass)


def classproperty(func):
    return ClassPropertyDescriptor(func)
