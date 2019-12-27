from contextvars import ContextVar

service = ContextVar("app", default=None)
settings = ContextVar("settings", default={})
