from pydantic import BaseModel


class PostgresFieldBase(BaseModel):
    """
    Extensions to python core types that specify addition arguments
    used by Postgres.

    """

    pass


class PostgresDateTime(PostgresFieldBase):
    """
    Extension to Python's datetime type that specifies additional Postgres-specific configuration.
    Used to customize the timezone behavior of datetime fields in Postgres.

    ```python {{sticky: True}}
    from iceaxe import Field, TableBase
    class Event(TableBase):
        id: int = Field(primary_key=True)
        created_at: datetime = Field(postgres_config=PostgresDateTime(timezone=True))
    ```
    """

    timezone: bool = False
    """
    Whether the datetime field should include timezone information in Postgres.
        If True, maps to TIMESTAMP WITH TIME ZONE.
        If False, maps to TIMESTAMP WITHOUT TIME ZONE.
        Defaults to False.

    """


class PostgresTime(PostgresFieldBase):
    """
    Extension to Python's time type that specifies additional Postgres-specific configuration.
    Used to customize the timezone behavior of time fields in Postgres.

    ```python {{sticky: True}}
    from iceaxe import Field, TableBase
    class Schedule(TableBase):
        id: int = Field(primary_key=True)
        start_time: time = Field(postgres_config=PostgresTime(timezone=True))
    ```
    """

    timezone: bool = False
    """
    Whether the time field should include timezone information in Postgres.
        If True, maps to TIME WITH TIME ZONE.
        If False, maps to TIME WITHOUT TIME ZONE.
        Defaults to False.

    """
