from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

from mnp.config import get_settings
from mnp.models import Base

config = context.config
if config.config_file_name is not None:
    # Keep loggers created before migrations run (e.g. in tests) enabled.
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = Base.metadata
# Tests pass their own database via config.attributes; otherwise use mnp settings.
database_url = config.attributes.get("database_url") or get_settings().database_url


def run_migrations_offline() -> None:
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(database_url, poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
