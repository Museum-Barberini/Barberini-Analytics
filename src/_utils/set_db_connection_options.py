import os

import luigi


def set_db_connection_options(task: luigi.Task) -> None:
    """
    Set the attributes host, database, user, and password to
    the values given in the etc/secrets/database.env file for the given task.
    Modifies the task in place.

    Usage:
        To use this method for a luigi.Task add the following __init__
        method to the task:

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                set_db_connection_options(self)

        Please note that if the class you are modifying is a subclass
        of luigi.contrib.postgres.CopyToTable you need to declare the
        attributes host, database, user, and password outside of __init__.
        You can do so with the following piece of code:

            host     = None
            database = None
            user     = None
            password = None

            def __init__(...):
                ...
    """
    task.host = os.environ['POSTGRES_HOST']
    task.database = os.environ['POSTGRES_DB']
    task.user = os.environ['POSTGRES_USER']
    task.password = os.environ['POSTGRES_PASSWORD']
