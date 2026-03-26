/**
 * Service-DataManager
 *
 * Manages the harness data directory: clean and recreate.
 *
 * The Facto and Target server databases (facto.db, target.db) live inside the
 * data directory and are owned by Service-ServerManager.  The DataManager's
 * job is simply to wipe and recreate the directory; the servers restart with
 * fresh databases after that step completes.
 *
 * The initDatabase() method is retained for the harness's own run-tracking
 * SQLite database (harness.db) which is separate from the server databases.
 */
const libFableServiceProviderBase = require('fable-serviceproviderbase');
const libFs = require('fs');
const libPath = require('path');
const libMeadowConnectionSQLite = require('meadow-connection-sqlite');

// Harness run-tracking schema (separate from facto.db / target.db)
const HARNESS_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS HarnessTestRun (
	IDHarnessTestRun INTEGER PRIMARY KEY AUTOINCREMENT,
	StartDate TEXT,
	EndDate TEXT,
	TotalDatasets INTEGER DEFAULT 0,
	PassedDatasets INTEGER DEFAULT 0,
	FailedDatasets INTEGER DEFAULT 0,
	Status TEXT DEFAULT 'Pending'
);

CREATE TABLE IF NOT EXISTS HarnessTestResult (
	IDHarnessTestResult INTEGER PRIMARY KEY AUTOINCREMENT,
	IDHarnessTestRun INTEGER DEFAULT 0,
	DatasetName TEXT,
	FilePath TEXT,
	ParsedCount INTEGER DEFAULT 0,
	LoadedCount INTEGER DEFAULT 0,
	VerifiedCount INTEGER DEFAULT 0,
	Status TEXT DEFAULT 'Pending',
	ErrorMessage TEXT,
	StartDate TEXT,
	EndDate TEXT
);
`;

class ServiceDataManager extends libFableServiceProviderBase
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);

		this.serviceType = 'HarnessDataManager';

		/** Raw better-sqlite3 db object — set after initDatabase() succeeds */
		this.db = null;
	}

	/**
	 * Remove the data directory and recreate it fresh.
	 * The servers must be stopped before calling this.
	 *
	 * @param {string}   pDataDir  - Absolute path to the data directory
	 * @param {function} fCallback - Called with (pError)
	 */
	cleanDataDir(pDataDir, fCallback)
	{
		try
		{
			if (libFs.existsSync(pDataDir))
			{
				this._rimraf(pDataDir);
			}
			libFs.mkdirSync(pDataDir, { recursive: true });
			this.fable.log.info(`DataManager: data dir cleaned and recreated at [${pDataDir}]`);
			return fCallback(null);
		}
		catch (pError)
		{
			this.fable.log.error(`DataManager: cleanDataDir failed — ${pError.message}`);
			return fCallback(pError);
		}
	}

	/**
	 * Initialize the harness run-tracking SQLite database at
	 * pDataDir/harness.db, creating schema tables if they do not exist.
	 *
	 * @param {string}   pDataDir  - Absolute path to the data directory
	 * @param {function} fCallback - Called with (pError)
	 */
	initDatabase(pDataDir, fCallback)
	{
		let tmpDBPath = libPath.join(pDataDir, 'harness.db');

		try
		{
			libFs.mkdirSync(pDataDir, { recursive: true });
		}
		catch (pMkdirError)
		{
			return fCallback(pMkdirError);
		}

		// If there was a previous connection, drop it before reconnecting
		if (this.db)
		{
			try { this.db.close(); } catch (pCloseError) { /* ignore */ }
			this.db = null;
		}

		// Point the SQLite provider at the harness tracking DB
		if (!this.fable.settings.SQLite)
		{
			this.fable.settings.SQLite = {};
		}
		this.fable.settings.SQLite.SQLiteFilePath = tmpDBPath;

		// Register the provider if not already registered
		this.fable.addAndInstantiateServiceTypeIfNotExists('MeadowSQLiteProvider', libMeadowConnectionSQLite);

		// Reconnect (provider re-reads the path from settings each time)
		this.fable.MeadowSQLiteProvider.connectAsync(
			(pConnectError) =>
			{
				if (pConnectError)
				{
					this.fable.log.error(`DataManager: SQLite connect failed — ${pConnectError.message}`);
					return fCallback(pConnectError);
				}

				try
				{
					this.fable.MeadowSQLiteProvider.db.exec(HARNESS_SCHEMA_SQL);
					this.db = this.fable.MeadowSQLiteProvider.db;
					this.fable.log.info(`DataManager: harness database initialized at [${tmpDBPath}]`);
					return fCallback(null);
				}
				catch (pSchemaError)
				{
					this.fable.log.error(`DataManager: schema creation failed — ${pSchemaError.message}`);
					return fCallback(pSchemaError);
				}
			});
	}

	/**
	 * Synchronous recursive directory removal (no external deps).
	 *
	 * @param {string} pDirPath - Directory to remove
	 */
	_rimraf(pDirPath)
	{
		if (!libFs.existsSync(pDirPath))
		{
			return;
		}

		let tmpStat = libFs.statSync(pDirPath);

		if (tmpStat.isFile() || tmpStat.isSymbolicLink())
		{
			libFs.unlinkSync(pDirPath);
			return;
		}

		let tmpEntries = libFs.readdirSync(pDirPath);
		for (let tmpEntry of tmpEntries)
		{
			this._rimraf(libPath.join(pDirPath, tmpEntry));
		}
		libFs.rmdirSync(pDirPath);
	}
}

module.exports = ServiceDataManager;
