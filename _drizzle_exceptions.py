try:
    from exceptions import Exception, StandardError, Warning
except ImportError:
    StandardError = Exception

class DrizzleError(StandardError):
    pass

class Error(DrizzleError):
    pass

class DatabaseError(Error):
    pass

class InternalError(DatabaseError):
    pass
