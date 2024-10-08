from pydantic import BaseModel


class PostgresFieldBase(BaseModel):
    """
    Extensions to python core types that specify addition arguments
    used by Postgres.

    """

    pass


class PostgresDateTime(PostgresFieldBase):
    timezone: bool = False


class PostgresTime(PostgresFieldBase):
    timezone: bool = False
