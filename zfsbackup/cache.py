import logging
import sqlite3


class Cache:
    MIGRATIONS = [
        # db version 1
        """
        BEGIN TRANSACTION;
        CREATE TABLE keep_snapshots (
            dataset TEXT,
            snapshot TEXT,
            count INT,
            UNIQUE(dataset, snapshot)
        );
        CREATE TABLE db_version (
            version INT
        );
        INSERT INTO db_version VALUES (1);
        COMMIT;
        """,
        # db version 2
        """
        BEGIN TRANSACTION;
        ALTER TABLE keep_snapshots RENAME to keep_snapshots_old;
        CREATE TABLE keep_snapshots (
            dataset TEXT NOT NULL,
            snapshot TEXT NOT NULL,
            count INT NOT NULL,
            UNIQUE(dataset, snapshot)
        );
        INSERT INTO keep_snapshots (dataset, snapshot, count)
            SELECT
                IFNULL(dataset, '') AS dataset,
                IFNULL(snapshot, '') AS snapshot,
                IFNULL(count, 0) AS count
            FROM
                keep_snapshots_old
            WHERE
                dataset IS NOT NULL
                OR snapshot IS NOT NULL;
        DROP TABLE keep_snapshots_old;
        UPDATE db_version SET version=2;
        COMMIT;
        """,
    ]

    def __init__(self, file: str, autocommit=True):
        self._file = file
        self._db: sqlite3.Connection = None
        self._autocommit = autocommit
        self._log = logging.getLogger("Cache")

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def open(self):
        if self._db:
            return
        self._log.debug("Opening cache file %s", self._file)
        self._db = sqlite3.connect(self._file)

    def close(self):
        if not self._db:
            return
        if self._autocommit:
            self.commit()
        self._log.debug("Closing cache file %s", self._file)
        self._db.close()
        self._db = None

    @property
    def db_version(self) -> int:
        try:
            cur = self._db.cursor()
            cur.execute("SELECT version FROM db_version")
            result = cur.fetchone()
            return result[0] if result else -1
        except sqlite3.OperationalError:
            return -1

    @property
    def is_current(self) -> bool:
        return self.db_version == len(self.MIGRATIONS)

    def commit(self):
        self._db.commit()

    def update_tables(self):
        version = self.db_version
        if version < 0:
            version = 0
        for migration in self.MIGRATIONS[version:]:
            self._db.executescript(migration)

    def snapshot_keep(self, dataset: str, snapshot: str) -> int:
        cur = self._db.cursor()
        cur.execute(
            "SELECT count FROM keep_snapshots WHERE dataset=? AND snapshot=?",
            [dataset, snapshot]
        )
        result = cur.fetchone()
        return result[0] if result else 0

    def snapshot_keep_increase(self, dataset: str, snapshot: str):
        cur = self._db.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO keep_snapshots
            VALUES (
                ?,
                ?,
                COALESCE(
                    (SELECT count FROM keep_snapshots
                    WHERE dataset=? AND snapshot=?),
                    0) + 1)
            """,
            [dataset, snapshot, dataset, snapshot]
        )
        if self._log.getEffectiveLevel() == logging.DEBUG:
            self._log.debug("Increased %s@%s count to %d",
                            dataset, snapshot,
                            self.snapshot_keep(dataset, snapshot))

    def snapshot_keep_decrease(self, dataset: str, snapshot: str):
        cur = self._db.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO keep_snapshots
            VALUES (
                ?,
                ?,
                COALESCE(
                    (SELECT count FROM keep_snapshots
                    WHERE dataset=? AND snapshot=?),
                    0) - 1)
            """,
            [dataset, snapshot, dataset, snapshot]
        )
        if self._log.getEffectiveLevel() == logging.DEBUG:
            self._log.debug("Decreased %s@%s count to %d",
                            dataset, snapshot,
                            self.snapshot_keep(dataset, snapshot))
