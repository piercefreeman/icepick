from pydantic_settings import BaseSettings

from iceaxe.modifications import MODIFICATION_TRACKER_VERBOSITY


class DatabaseConfig(BaseSettings):
    """
    Configuration settings for PostgreSQL database connection.
    This class uses Pydantic's BaseSettings to manage environment-based configuration.
    """

    POSTGRES_HOST: str
    """
    The hostname where the PostgreSQL server is running.
    This can be a domain name or IP address (e.g., 'localhost' or '127.0.0.1').
    """

    POSTGRES_USER: str
    """
    The username to authenticate with the PostgreSQL server.
    This user should have appropriate permissions for the database operations.
    """

    POSTGRES_PASSWORD: str
    """
    The password for authenticating the PostgreSQL user.
    This should be kept secure and not exposed in code or version control.
    """

    POSTGRES_DB: str
    """
    The name of the PostgreSQL database to connect to.
    This database should exist on the server before attempting connection.
    """

    POSTGRES_PORT: int = 5432
    """
    The port number where PostgreSQL server is listening.
    Defaults to the standard PostgreSQL port 5432 if not specified.
    """

    ICEAXE_UNCOMMITTED_VERBOSITY: MODIFICATION_TRACKER_VERBOSITY | None = None
    """
    The verbosity level for uncommitted modifications.
    If set to None, uncommitted modifications will not be tracked.
    """
