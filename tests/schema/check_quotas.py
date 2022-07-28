"""Schema assertions."""

from db_test import DatabaseTestCase


class CheckQuotas(DatabaseTestCase):
    """
    Make quick assertions about the schema.

    Meant to be run after the database has been filled, e. g. after a minimal
    pipeline run has been executed. Requires $POSTGRES_DB_TEMPLATE to point to
    a prepared database.
    NOTE that if these tests get too slow, we could override setup_database to
    prevent copying the database for each read-only access. However, at the
    moment YAGNI.
    """

    def test_capacities(self):
        """At least one capacity is present."""
        self.assertGreater(
            self.db_connector.query('SELECT COUNT(*) FROM gomus_capacity'),
            0,
            msg="No capacity is present"
        )

    def test_quotas(self):
        """At least one quota is present."""
        self.assertGreater(
            self.db_connector.query('SELECT COUNT(*) FROM gomus_quota'),
            0,
            msg="No quota is present"
        )
