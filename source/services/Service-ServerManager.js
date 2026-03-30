/**
 * Service-ServerManager
 *
 * Manages three embedded HTTP servers used by the harness integration tests:
 *
 *   Facto Server              — port 8420  — RetoldFacto data warehouse (Sources, Datasets, Records, Projections)
 *   Meadow-Integration Server — port 8421  — Parsing and transformation service
 *   Ultravisor Server         — port 8422  — Workflow execution engine + beacon coordinator
 *
 * Facto uses RetoldFacto with full web UI and meadow endpoints.
 * Meadow-Integration provides FileParser and TabularTransform as a remote service.
 * Ultravisor uses the full UltravisorAPIServer service.
 * After all servers start, Facto and Meadow-Integration register as beacons
 * with Ultravisor so that workflow operations can dispatch work to them.
 *
 * Usage:
 *   serverManager.startAll(pDataDir, (err) => { ... });
 *   serverManager.stopAll((err) => { ... });
 *   serverManager.restartAll(pDataDir, (err) => { ... });
 */
'use strict';

const libPath = require('path');
const libFs = require('fs');
const libPict = require('pict');
const libOrator = require('orator');
const libOratorRestify = require('orator-serviceserver-restify');
const libOratorStaticServer = require('orator-static-server');
const libMeadow = require('meadow');
const libMeadowEndpoints = require('meadow-endpoints');
const libMeadowConnectionSQLite = require('meadow-connection-sqlite');
const libFableServiceProviderBase = require('fable-serviceproviderbase');
const libUltravisor = require('ultravisor');
const libBeaconService = require('ultravisor-beacon');
const libUltravisorAPIServer = require('ultravisor/source/web_server/Ultravisor-API-Server.cjs');
const libRetoldFacto = require('retold-facto');
const libMeadowIntegration = require('meadow-integration');
const libTabularTransform = require('meadow-integration/source/services/tabular/Service-TabularTransform.js');
const libMeadowIntegrationFileParser = require('meadow-integration/source/services/parser/Service-FileParser.js');

// ── Port constants ──────────────────────────────────────────────────────────────

const FACTO_PORT = 8420;
const INTEGRATION_PORT = 8421;
const ULTRAVISOR_PORT = 8422;

// ── Port availability check ──────────────────────────────────────────────────────
// Probe the port with a temporary net.Server before Restify binds.
// This prevents the unhandled 'error' event crash from Restify when ports
// are already in use (e.g. a previous run killed with Ctrl-C).

const libNet = require('net');

function _checkPortAvailable(pPort, fCallback)
{
	let tmpServer = libNet.createServer();
	tmpServer.once('error',
		(pError) =>
		{
			if (pError.code === 'EADDRINUSE')
			{
				return fCallback(new Error(`Port ${pPort} is already in use. Kill the stale process or choose a different port.`));
			}
			return fCallback(pError);
		});
	tmpServer.once('listening',
		() =>
		{
			tmpServer.close(() => { fCallback(null); });
		});
	tmpServer.listen(pPort);
}

// ── ServiceServerManager ────────────────────────────────────────────────────────

class ServiceServerManager extends libFableServiceProviderBase
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);

		this.serviceType = 'HarnessServerManager';

		this._factoFable           = null;
		this._factoFactoService    = null;
		this._integrationFable     = null;
		this._integrationOrator    = null;
		this._ultravisorFable      = null;
		this._ultravisorOrator     = null;

		this.factoPort       = FACTO_PORT;
		this.integrationPort = INTEGRATION_PORT;
		this.ultravisorPort  = ULTRAVISOR_PORT;

		this.factoRunning       = false;
		this.integrationRunning = false;
		this.ultravisorRunning  = false;

		this._factoBeacon       = null;
		this._integrationBeacon = null;
	}

	// ─────────────────────────────────────────────
	//  Facto server
	// ─────────────────────────────────────────────

	_startFactoServer(pDataDir, fCallback)
	{
		let tmpDBPath = libPath.join(pDataDir, 'facto.db');

		this._factoFable = new libPict(
			{
				Product: 'HarnessFactoServer',
				LogNoisiness: 0,
				APIServerPort: FACTO_PORT,
				SQLite: { SQLiteFilePath: tmpDBPath },
				LogStreams: [],
			});

		// SQLite provider — RetoldFacto expects this to be connected before init
		this._factoFable.serviceManager.addServiceType('MeadowSQLiteProvider', libMeadowConnectionSQLite);
		this._factoFable.serviceManager.instantiateServiceProvider('MeadowSQLiteProvider');

		this._factoFable.MeadowSQLiteProvider.connectAsync(
			(pConnectError) =>
			{
				if (pConnectError)
				{
					return fCallback(new Error(`Facto SQLite connect failed: ${pConnectError.message}`));
				}

				// Meadow audit columns need a User table — Facto's schema doesn't include one
				try
				{
					this._factoFable.MeadowSQLiteProvider.db.exec(
						`CREATE TABLE IF NOT EXISTS User (
							IDUser INTEGER PRIMARY KEY AUTOINCREMENT,
							GUIDUser TEXT DEFAULT '',
							LoginID TEXT DEFAULT ''
						)`);
					this._factoFable.MeadowSQLiteProvider.db.prepare(
						`INSERT OR IGNORE INTO User (IDUser, GUIDUser, LoginID) VALUES (1, 'system', 'system')`
					).run();
				}
				catch (pUserError)
				{
					this.fable.log.warn(`[ServerManager] Could not create User table: ${pUserError.message}`);
				}

				_checkPortAvailable(FACTO_PORT,
					(pPortError) =>
					{
						if (pPortError)
						{
							return fCallback(new Error(`Facto: ${pPortError.message}`));
						}

						// Resolve the model path from the retold-facto module
						let tmpFactoModulePath = libPath.dirname(require.resolve('retold-facto/package.json'));
						let tmpModelPath = libPath.join(tmpFactoModulePath, 'test', 'model') + '/';

						// Register and instantiate RetoldFacto — handles schema, endpoints, web UI, Orator
						this._factoFable.serviceManager.addServiceType('RetoldFacto', libRetoldFacto);
						this._factoFactoService = this._factoFable.serviceManager.instantiateServiceProvider('RetoldFacto',
							{
								StorageProvider: 'SQLite',
								AutoStartOrator: true,
								AutoCreateSchema: true,
								FullMeadowSchemaPath: tmpModelPath,
								FullMeadowSchemaFilename: 'MeadowModel-Extended.json',
								Endpoints:
								{
									MeadowEndpoints: true,
									SourceManager: true,
									RecordManager: true,
									DatasetManager: true,
									IngestEngine: true,
									ProjectionEngine: true,
									CatalogManager: true,
									StoreConnectionManager: true,
									SourceFolderScanner: false,
									WebUI: true,
								},
								Facto:
								{
									RoutePrefix: '/facto',
								},
							});

						this._factoFactoService.initializeService(
							(pInitError) =>
							{
								if (pInitError)
								{
									return fCallback(new Error(`Facto init failed: ${pInitError.message}`));
								}
								this.factoRunning = true;
								this.fable.log.info(`[ServerManager] Facto server listening on port ${FACTO_PORT}`);

								// Seed a StoreConnection for projections so the
								// Facto web UI has a target store available
								let tmpProjectionsDBPath = libPath.join(pDataDir, 'projections.db');
								let tmpConnQuery = this._factoFable.DAL.StoreConnection.query.clone();
								tmpConnQuery.addRecord(
								{
									Name: 'Harness Projections (SQLite)',
									Type: 'SQLite',
									Config: JSON.stringify({ SQLiteFilePath: tmpProjectionsDBPath }),
									Status: 'OK'
								});
								this._factoFable.DAL.StoreConnection.doCreate(tmpConnQuery,
									(pConnError) =>
									{
										if (pConnError)
										{
											this.fable.log.warn(`[ServerManager] StoreConnection seed: ${pConnError.message}`);
										}
										return fCallback(null);
									});
							});
					});
			});
	}

	_stopFactoServer(fCallback)
	{
		if (!this.factoRunning || !this._factoFable)
		{
			this.factoRunning       = false;
			this._factoFable        = null;
			this._factoFactoService = null;
			return fCallback(null);
		}

		try
		{
			let tmpServer = this._factoFable.OratorServiceServer
				&& this._factoFable.OratorServiceServer.server;

			if (tmpServer && typeof tmpServer.close === 'function')
			{
				tmpServer.close(
					() =>
					{
						this.factoRunning       = false;
						this._factoFable        = null;
						this._factoFactoService = null;
						return fCallback(null);
					});
				return;
			}
		}
		catch (pError)
		{
			// fall through
		}

		this.factoRunning       = false;
		this._factoFable        = null;
		this._factoFactoService = null;
		return fCallback(null);
	}

	// ─────────────────────────────────────────────
	//  Meadow-Integration server
	// ─────────────────────────────────────────────

	_startIntegrationServer(pDataDir, fCallback)
	{
		let tmpIntegrationFable = new libPict(
			{
				Product: 'HarnessIntegration',
				LogNoisiness: 0,
				LogStreams: [],
				APIServerPort: INTEGRATION_PORT,
			});

		this._integrationFable = tmpIntegrationFable;

		// Register services needed for parsing and transformation
		tmpIntegrationFable.serviceManager.addServiceType('MeadowIntegrationFileParser', libMeadowIntegrationFileParser);
		tmpIntegrationFable.serviceManager.instantiateServiceProvider('MeadowIntegrationFileParser');
		tmpIntegrationFable.serviceManager.addServiceType('TabularTransform', libTabularTransform);
		tmpIntegrationFable.serviceManager.instantiateServiceProvider('TabularTransform');

		// Set up Orator for HTTP
		tmpIntegrationFable.serviceManager.addServiceType('OratorServiceServer', libOratorRestify);
		tmpIntegrationFable.serviceManager.addServiceType('Orator', libOrator);
		tmpIntegrationFable.serviceManager.addServiceType('OratorStaticServer', libOratorStaticServer);
		tmpIntegrationFable.serviceManager.instantiateServiceProvider('OratorServiceServer', {});

		let tmpOrator = tmpIntegrationFable.serviceManager.instantiateServiceProvider('Orator', {});
		this._integrationOrator = tmpOrator;

		_checkPortAvailable(INTEGRATION_PORT,
			(pPortError) =>
			{
				if (pPortError)
				{
					return fCallback(new Error(`Integration: ${pPortError.message}`));
				}

				tmpOrator.initialize(
					(pInitError) =>
					{
						if (pInitError)
						{
							return fCallback(new Error(`Integration init: ${pInitError.message}`));
						}

						tmpIntegrationFable.OratorServiceServer.server.use(
							tmpIntegrationFable.OratorServiceServer.bodyParser());

						// Register a simple status endpoint
						tmpIntegrationFable.OratorServiceServer.get('/status',
							(pRequest, pResponse, fNext) =>
							{
								pResponse.send({ Service: 'meadow-integration', Status: 'Running', Port: INTEGRATION_PORT });
								return fNext();
							});

						// Serve the mapping demo and docs web apps
						let tmpMIRoot = libPath.resolve(__dirname, '..', '..', '..', '..', 'meadow', 'meadow-integration');
						let tmpStaticServer = tmpIntegrationFable.serviceManager.instantiateServiceProvider('OratorStaticServer');
						tmpStaticServer.addStaticRoute(
							libPath.join(tmpMIRoot, 'example-applications', 'mapping-demo', 'web'),
							'index.html', '/mapping/*', '/mapping/');
						tmpStaticServer.addStaticRoute(
							libPath.join(tmpMIRoot, 'docs'),
							'index.html', '/docs/*', '/docs/');

						tmpOrator.startWebServer(
							(pStartError) =>
							{
								if (pStartError)
								{
									return fCallback(new Error(`Integration start: ${pStartError.message}`));
								}

								this.integrationRunning = true;
								this.fable.log.info(`[ServerManager] Meadow-Integration server listening on port ${INTEGRATION_PORT}`);
								return fCallback(null);
							});
					});
			});
	}

	_stopIntegrationServer(fCallback)
	{
		if (!this.integrationRunning || !this._integrationFable)
		{
			this.integrationRunning  = false;
			this._integrationFable   = null;
			this._integrationOrator  = null;
			return fCallback(null);
		}

		try
		{
			let tmpServer = this._integrationFable.OratorServiceServer
				&& this._integrationFable.OratorServiceServer.server;

			if (tmpServer && typeof tmpServer.close === 'function')
			{
				tmpServer.close(
					() =>
					{
						this.integrationRunning  = false;
						this._integrationFable   = null;
						this._integrationOrator  = null;
						return fCallback(null);
					});
				return;
			}
		}
		catch (pCloseError)
		{
			// fall through
		}

		this.integrationRunning  = false;
		this._integrationFable   = null;
		this._integrationOrator  = null;
		return fCallback(null);
	}

	// ─────────────────────────────────────────────
	//  Ultravisor server
	// ─────────────────────────────────────────────

	_startUltravisorServer(pDataDir, fCallback)
	{
		this._ultravisorFable = new libPict(
			{
				Product: 'HarnessUltravisorServer',
				LogNoisiness: 0,
				APIServerPort: ULTRAVISOR_PORT,
				LogStreams: [],
			});

		// HypervisorState calls gatherProgramConfiguration which comes from
		// pict-service-commandlineutility.  The harness doesn't use the CLI
		// utility, so provide a stub that loads .ultravisor.json from the
		// Ultravisor module directory — same file the real CLI reads.
		if (typeof this._ultravisorFable.gatherProgramConfiguration !== 'function')
		{
			let tmpUltravisorRoot = libPath.resolve(__dirname, '..', '..', '..', 'ultravisor');
			let tmpConfigPath = libPath.join(tmpUltravisorRoot, '.ultravisor.json');
			let tmpConfig = {};
			try
			{
				tmpConfig = JSON.parse(libFs.readFileSync(tmpConfigPath, 'utf8'));
			}
			catch (pConfigError)
			{
				this.fable.log.warn(`[ServerManager] Could not load ${tmpConfigPath}: ${pConfigError.message}`);
			}
			tmpConfig.UltravisorAPIServerPort = ULTRAVISOR_PORT;
			tmpConfig.UltravisorWebInterfacePath = libPath.join(tmpUltravisorRoot, 'webinterface', 'dist');
			this._ultravisorFable.ProgramConfiguration = tmpConfig;
			this._ultravisorFable.gatherProgramConfiguration = function ()
			{
				return { GatherPhases: [{ Phase: 'Harness', Path: tmpConfigPath }], Settings: tmpConfig };
			};
		}

		// Register core Ultravisor services
		this._ultravisorFable.serviceManager.addServiceType('UltravisorTaskTypeRegistry', libUltravisor.TaskTypeRegistry);
		this._ultravisorFable.serviceManager.addServiceType('UltravisorStateManager', libUltravisor.StateManager);
		this._ultravisorFable.serviceManager.addServiceType('UltravisorExecutionEngine', libUltravisor.ExecutionEngine);
		this._ultravisorFable.serviceManager.addServiceType('UltravisorExecutionManifest', libUltravisor.ExecutionManifest);
		this._ultravisorFable.serviceManager.addServiceType('UltravisorHypervisorState', libUltravisor.HypervisorState);
		this._ultravisorFable.serviceManager.addServiceType('UltravisorHypervisor', libUltravisor.Hypervisor);
		this._ultravisorFable.serviceManager.addServiceType('UltravisorBeaconCoordinator', libUltravisor.BeaconCoordinator);

		// Instantiate core services
		this._ultravisorFable.serviceManager.instantiateServiceProvider('UltravisorTaskTypeRegistry');
		this._ultravisorFable.serviceManager.instantiateServiceProvider('UltravisorStateManager');
		this._ultravisorFable.serviceManager.instantiateServiceProvider('UltravisorExecutionEngine');
		this._ultravisorFable.serviceManager.instantiateServiceProvider('UltravisorExecutionManifest');
		this._ultravisorFable.serviceManager.instantiateServiceProvider('UltravisorHypervisorState');
		this._ultravisorFable.serviceManager.instantiateServiceProvider('UltravisorHypervisor');
		this._ultravisorFable.serviceManager.instantiateServiceProvider('UltravisorBeaconCoordinator');

		// Register built-in task types (50+ built-in tasks: meadow, data-transform, etc.)
		this._ultravisorFable.UltravisorTaskTypeRegistry.registerBuiltInTaskTypes();

		// Load operation definitions from the operations/ directory
		let tmpOpsDir = libPath.resolve(__dirname, '..', '..', 'operations');
		try
		{
			let tmpOpFiles = libFs.readdirSync(tmpOpsDir).filter(
				(pFile) =>
				{
					return pFile.endsWith('.json');
				});

			for (let i = 0; i < tmpOpFiles.length; i++)
			{
				let tmpOpData = JSON.parse(libFs.readFileSync(libPath.join(tmpOpsDir, tmpOpFiles[i]), 'utf8'));
				// updateOperation is the create-or-update method; callback is required
				this._ultravisorFable.UltravisorHypervisorState.updateOperation(tmpOpData,
					(pOpError) =>
					{
						if (pOpError)
						{
							this.fable.log.warn(`[ServerManager] Failed to load operation ${tmpOpFiles[i]}: ${pOpError.message}`);
						}
					});
				this.fable.log.info(`[ServerManager] Loaded operation: ${tmpOpFiles[i]} (Hash: ${tmpOpData.Hash})`);
			}
		}
		catch (pOpsError)
		{
			this.fable.log.warn(`[ServerManager] Could not load operations from ${tmpOpsDir}: ${pOpsError.message}`);
		}

		// Register and instantiate the UltravisorAPIServer service
		this._ultravisorFable.serviceManager.addServiceType('UltravisorAPIServer', libUltravisorAPIServer);
		let tmpAPIServer = this._ultravisorFable.serviceManager.instantiateServiceProvider('UltravisorAPIServer');

		_checkPortAvailable(ULTRAVISOR_PORT,
			(pPortError) =>
			{
				if (pPortError)
				{
					return fCallback(new Error(`Ultravisor: ${pPortError.message}`));
				}
				tmpAPIServer.start(
					(pStartError) =>
					{
						if (pStartError)
						{
							return fCallback(new Error(`Ultravisor server start failed: ${pStartError.message}`));
						}
						this.ultravisorRunning = true;
						this.fable.log.info(`[ServerManager] Ultravisor server listening on port ${ULTRAVISOR_PORT}`);
						return fCallback(null);
					});
			});
	}

	_stopUltravisorServer(fCallback)
	{
		if (!this.ultravisorRunning || !this._ultravisorFable)
		{
			this.ultravisorRunning = false;
			this._ultravisorFable  = null;
			this._ultravisorOrator = null;
			return fCallback(null);
		}

		// Close the WebSocket server if the API server created one
		try
		{
			let tmpAPIServer = this._ultravisorFable.UltravisorAPIServer;
			if (tmpAPIServer && tmpAPIServer._WebSocketServer)
			{
				tmpAPIServer._WebSocketServer.close();
			}
		}
		catch (pWSError)
		{
			// non-fatal
		}

		try
		{
			let tmpServer = this._ultravisorFable.OratorServiceServer
				&& this._ultravisorFable.OratorServiceServer.server;

			if (tmpServer && typeof tmpServer.close === 'function')
			{
				tmpServer.close(
					() =>
					{
						this.ultravisorRunning = false;
						this._ultravisorFable  = null;
						this._ultravisorOrator = null;
						return fCallback(null);
					});
				return;
			}
		}
		catch (pError)
		{
			// fall through
		}

		this.ultravisorRunning = false;
		this._ultravisorFable  = null;
		this._ultravisorOrator = null;
		return fCallback(null);
	}

	// ─────────────────────────────────────────────
	//  Facto beacon registration
	// ─────────────────────────────────────────────

	_registerFactoBeacon(fCallback)
	{
		// Register beacon service type on the facto fable instance
		this._factoFable.addServiceTypeIfNotExists('UltravisorBeacon', libBeaconService);

		this._factoBeacon = this._factoFable.instantiateServiceProviderWithoutRegistration('UltravisorBeacon',
			{
				ServerURL: `http://localhost:${ULTRAVISOR_PORT}`,
				Name: 'retold-facto',
				MaxConcurrent: 5,
			});

		// Capture the facto fable for closures
		let tmpFactoFable = this._factoFable;

		// Register FactoData capability
		this._factoBeacon.registerCapability(
			{
				Capability: 'FactoData',
				Name: 'FactoDataProvider',
				actions:
				{
					'CreateSource':
					{
						Description: 'Create a Source entity',
						SettingsSchema:
						[
							{ Name: 'Name', DataType: 'String', Required: true },
							{ Name: 'Type', DataType: 'String' },
							{ Name: 'URL', DataType: 'String' },
						],
						Handler: function (pWorkItem, pContext, fHandlerCallback)
						{
							let tmpSettings = pWorkItem.Settings || {};
							let tmpName = tmpSettings.Name || '';
							let tmpHash = tmpName.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
							let tmpQuery = tmpFactoFable.DAL.Source.query.clone();
							tmpQuery.addRecord({ Name: tmpName, Hash: tmpHash, Type: tmpSettings.Type || '', URL: tmpSettings.URL || '', Active: 1 });
							tmpFactoFable.DAL.Source.doCreate(tmpQuery,
								(pError, pQuery, pQueryRead, pRecord) =>
								{
									if (pError)
									{
										return fHandlerCallback(pError);
									}
									return fHandlerCallback(null, { Outputs: { Created: pRecord } });
								});
						}
					},
					'CreateDataset':
					{
						Description: 'Create a Dataset entity',
						SettingsSchema:
						[
							{ Name: 'Name', DataType: 'String', Required: true },
							{ Name: 'Type', DataType: 'String' },
						],
						Handler: function (pWorkItem, pContext, fHandlerCallback)
						{
							let tmpSettings = pWorkItem.Settings || {};
							let tmpName = tmpSettings.Name || '';
							let tmpHash = tmpName.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
							let tmpQuery = tmpFactoFable.DAL.Dataset.query.clone();
							tmpQuery.addRecord({ Name: tmpName, Hash: tmpHash, Type: tmpSettings.Type || '' });
							tmpFactoFable.DAL.Dataset.doCreate(tmpQuery,
								(pError, pQuery, pQueryRead, pRecord) =>
								{
									if (pError)
									{
										return fHandlerCallback(pError);
									}
									return fHandlerCallback(null, { Outputs: { Created: pRecord } });
								});
						}
					},
					'CreateIngestJob':
					{
						Description: 'Create an IngestJob entity',
						SettingsSchema:
						[
							{ Name: 'IDSource', DataType: 'Integer', Required: true },
							{ Name: 'IDDataset', DataType: 'Integer', Required: true },
							{ Name: 'Status', DataType: 'String' },
						],
						Handler: function (pWorkItem, pContext, fHandlerCallback)
						{
							let tmpSettings = pWorkItem.Settings || {};
							let tmpIDSource = tmpSettings.IDSource || 0;
							let tmpIDDataset = tmpSettings.IDDataset || 0;

							// Ensure DatasetSource link exists so Stats shows Sources
							let tmpDSQuery = tmpFactoFable.DAL.DatasetSource.query.clone();
							tmpDSQuery.addFilter('IDDataset', tmpIDDataset);
							tmpDSQuery.addFilter('IDSource', tmpIDSource);
							tmpFactoFable.DAL.DatasetSource.doReads(tmpDSQuery,
								(pDSError, pDSQuery, pExisting) =>
								{
									let tmpCreateJob = () =>
									{
										let tmpQuery = tmpFactoFable.DAL.IngestJob.query.clone();
										tmpQuery.addRecord({ IDSource: tmpIDSource, IDDataset: tmpIDDataset, Status: tmpSettings.Status || 'Pending' });
										tmpFactoFable.DAL.IngestJob.doCreate(tmpQuery,
											(pError, pQuery, pQueryRead, pRecord) =>
											{
												if (pError)
												{
													return fHandlerCallback(pError);
												}
												return fHandlerCallback(null, { Outputs: { Created: pRecord } });
											});
									};

									if (!pDSError && (!pExisting || pExisting.length === 0) && tmpIDSource && tmpIDDataset)
									{
										// Create the DatasetSource link
										let tmpLinkQuery = tmpFactoFable.DAL.DatasetSource.query.clone();
										tmpLinkQuery.addRecord({ IDDataset: tmpIDDataset, IDSource: tmpIDSource, ReliabilityWeight: 0.5 });
										tmpFactoFable.DAL.DatasetSource.doCreate(tmpLinkQuery,
											(pLinkError) =>
											{
												if (pLinkError)
												{
													tmpLog.warn(`DatasetSource link failed: ${pLinkError.message}`);
												}
												tmpCreateJob();
											});
									}
									else
									{
										tmpCreateJob();
									}
								});
						}
					},
					'CreateRecord':
					{
						Description: 'Create a Record entity',
						SettingsSchema:
						[
							{ Name: 'IDDataset', DataType: 'Integer', Required: true },
							{ Name: 'IDSource', DataType: 'Integer' },
							{ Name: 'Content', DataType: 'String' },
						],
						Handler: function (pWorkItem, pContext, fHandlerCallback)
						{
							let tmpSettings = pWorkItem.Settings || {};
							let tmpQuery = tmpFactoFable.DAL.Record.query.clone();
							tmpQuery.addRecord({ IDDataset: tmpSettings.IDDataset || 0, IDSource: tmpSettings.IDSource || 0, Content: tmpSettings.Content || '' });
							tmpFactoFable.DAL.Record.doCreate(tmpQuery,
								(pError, pQuery, pQueryRead, pRecord) =>
								{
									if (pError)
									{
										return fHandlerCallback(pError);
									}
									return fHandlerCallback(null, { Outputs: { Created: pRecord } });
								});
						}
					},
					'ReadRecords':
					{
						Description: 'Read Record entities with optional filter',
						SettingsSchema:
						[
							{ Name: 'IDDataset', DataType: 'Integer' },
							{ Name: 'IDSource', DataType: 'Integer' },
						],
						Handler: function (pWorkItem, pContext, fHandlerCallback)
						{
							let tmpSettings = pWorkItem.Settings || {};
							let tmpQuery = tmpFactoFable.DAL.Record.query.clone();
							if (tmpSettings.IDDataset)
							{
								tmpQuery.addFilter('IDDataset', tmpSettings.IDDataset);
							}
							if (tmpSettings.IDSource)
							{
								tmpQuery.addFilter('IDSource', tmpSettings.IDSource);
							}
							tmpFactoFable.DAL.Record.doReads(tmpQuery,
								(pError, pQuery, pRecords) =>
								{
									if (pError)
									{
										return fHandlerCallback(pError);
									}
									return fHandlerCallback(null, { Outputs: { Records: pRecords } });
								});
						}
					},
					'UpdateIngestJob':
					{
						Description: 'Update an IngestJob entity',
						SettingsSchema:
						[
							{ Name: 'IDIngestJob', DataType: 'Integer', Required: true },
							{ Name: 'Status', DataType: 'String' },
							{ Name: 'RecordsProcessed', DataType: 'Integer' },
							{ Name: 'RecordsCreated', DataType: 'Integer' },
						],
						Handler: function (pWorkItem, pContext, fHandlerCallback)
						{
							let tmpSettings = pWorkItem.Settings || {};
							let tmpQuery = tmpFactoFable.DAL.IngestJob.query.clone();
							tmpQuery.addFilter('IDIngestJob', tmpSettings.IDIngestJob);
							let tmpUpdateRecord = { IDIngestJob: tmpSettings.IDIngestJob };
							if (tmpSettings.Status)
							{
								tmpUpdateRecord.Status = tmpSettings.Status;
							}
							if (tmpSettings.RecordsProcessed !== undefined)
							{
								tmpUpdateRecord.RecordsProcessed = tmpSettings.RecordsProcessed;
							}
							if (tmpSettings.RecordsCreated !== undefined)
							{
								tmpUpdateRecord.RecordsCreated = tmpSettings.RecordsCreated;
							}
							tmpQuery.addRecord(tmpUpdateRecord);
							tmpFactoFable.DAL.IngestJob.doUpdate(tmpQuery,
								(pError, pQuery, pQueryRead, pRecord) =>
								{
									if (pError)
									{
										return fHandlerCallback(pError);
									}
									return fHandlerCallback(null, { Outputs: { Updated: pRecord } });
								});
						}
					},
					'BulkCreateRecords':
					{
						Description: 'Create multiple Record entities in bulk',
						SettingsSchema:
						[
							{ Name: 'Records', DataType: 'Array', Required: true },
							{ Name: 'IDDataset', DataType: 'Integer' },
							{ Name: 'IDSource', DataType: 'Integer' },
						],
						Handler: function (pWorkItem, pContext, fHandlerCallback)
						{
							let tmpSettings = pWorkItem.Settings || {};
							let tmpRecords = tmpSettings.Records || [];
							let tmpIDDataset = parseInt(tmpSettings.IDDataset, 10) || 0;
							let tmpIDSource = parseInt(tmpSettings.IDSource, 10) || 0;
							let tmpDatasetName = tmpSettings.DatasetName || '';
							let tmpCreatedCount = 0;
							let tmpIndex = 0;
							// Unwind the call stack every BATCH_SIZE records to
							// prevent "Maximum call stack size exceeded" with
							// synchronous SQLite callbacks.
							let BATCH_SIZE = 50;

							let tmpCreateNext = function ()
							{
								if (tmpIndex >= tmpRecords.length)
								{
									// Emit final partial batch
									let tmpRemainder = tmpIndex % BATCH_SIZE;
									if (tmpRemainder > 0 && tmpFactoFable.ThroughputMonitor)
									{
										tmpFactoFable.ThroughputMonitor.recordEvent('written', tmpRemainder, tmpDatasetName);
									}
									return fHandlerCallback(null, { Outputs: { Count: tmpCreatedCount } });
								}
								let tmpRecordData = tmpRecords[tmpIndex];
								tmpIndex++;

								// If the record doesn't have a Content field, it's a raw
								// parsed object from the meadow-integration beacon.
								// Wrap it in a Facto Record envelope.
								if (tmpRecordData.Content === undefined)
								{
									tmpRecordData =
									{
										Type:       'HarnessRecord',
										IngestDate: new Date().toISOString(),
										Version:    1,
										Content:    JSON.stringify(tmpRecordData),
									};
								}

								// Inject IDDataset/IDSource from upstream tasks
								if (tmpIDDataset && !tmpRecordData.IDDataset)
								{
									tmpRecordData.IDDataset = tmpIDDataset;
								}
								if (tmpIDSource && !tmpRecordData.IDSource)
								{
									tmpRecordData.IDSource = tmpIDSource;
								}
								let tmpQuery = tmpFactoFable.DAL.Record.query.clone();
								tmpQuery.addRecord(tmpRecordData);
								tmpFactoFable.DAL.Record.doCreate(tmpQuery,
									(pError, pQuery, pQueryRead, pRecord) =>
									{
										if (pError)
										{
											tmpLog.warn(`BulkCreate error at index ${tmpIndex - 1}: ${pError.message}`);
											// Continue on error — don't abort the whole batch
										}
										else
										{
											tmpCreatedCount++;
										}
										// Unwind the stack periodically and emit throughput events
										if (tmpIndex % BATCH_SIZE === 0)
										{
											// Emit throughput event for this batch
											if (tmpFactoFable.ThroughputMonitor)
											{
												tmpFactoFable.ThroughputMonitor.recordEvent('written', BATCH_SIZE, tmpDatasetName);
											}
											return setImmediate(tmpCreateNext);
										}
										tmpCreateNext();
									});
							};
							tmpCreateNext();
						}
					}
				}
			});

		// ── FactoTransform capability ─────────────────────────────
		this._factoBeacon.registerCapability(
			{
				Capability: 'FactoTransform',
				Name: 'FactoTransformProvider',
				actions:
				{
					'ApplyMapping':
					{
						Description: 'Apply a projection mapping to a set of records',
						SettingsSchema:
						[
							{ Name: 'IDProjectionMapping', DataType: 'Integer', Required: true },
							{ Name: 'Records', DataType: 'Array' },
						],
						Handler: function (pWorkItem, pContext, fHandlerCallback)
						{
							let tmpSettings = pWorkItem.Settings || {};
							let tmpIDMapping = parseInt(tmpSettings.IDProjectionMapping, 10);
							let tmpRecords = tmpSettings.Records || [];

							// Load the ProjectionMapping to get MappingConfiguration
							let tmpQuery = tmpFactoFable.DAL.ProjectionMapping.query.clone();
							tmpQuery.addFilter('IDProjectionMapping', tmpIDMapping);
							tmpFactoFable.DAL.ProjectionMapping.doRead(tmpQuery,
								(pError, pQuery, pMapping) =>
								{
									if (pError || !pMapping || !pMapping.IDProjectionMapping)
									{
										return fHandlerCallback(pError || new Error('ProjectionMapping not found'));
									}

									let tmpMappingConfig = {};
									try { tmpMappingConfig = JSON.parse(pMapping.MappingConfiguration || '{}'); }
									catch (e) { return fHandlerCallback(new Error('Invalid MappingConfiguration JSON')); }

									// Transform each record using the mapping config
									let tmpTransformed = [];
									for (let i = 0; i < tmpRecords.length; i++)
									{
										let tmpRecord = tmpRecords[i];
										let tmpContent = tmpRecord.Content || tmpRecord;
										if (typeof tmpContent === 'string')
										{
											try { tmpContent = JSON.parse(tmpContent); }
											catch (e) { continue; }
										}

										let tmpMapped = {};
										let tmpMappings = tmpMappingConfig.Mappings || {};
										let tmpKeys = Object.keys(tmpMappings);
										for (let j = 0; j < tmpKeys.length; j++)
										{
											let tmpTargetField = tmpKeys[j];
											let tmpTemplate = tmpMappings[tmpTargetField];
											if (tmpTemplate && tmpTemplate.indexOf('{~') >= 0)
											{
												// Resolve template expression
												tmpMapped[tmpTargetField] = tmpFactoFable.parseTemplate(tmpTemplate, { Record: tmpContent });
											}
											else
											{
												tmpMapped[tmpTargetField] = tmpTemplate;
											}
										}
										tmpTransformed.push(tmpMapped);
									}

									return fHandlerCallback(null, { Outputs: { Comprehension: tmpTransformed, ParsedRowCount: tmpRecords.length, UniqueCount: tmpTransformed.length } });
								});
						}
					}
				}
			});

		// ── FactoDeploy capability ────────────────────────────────
		this._factoBeacon.registerCapability(
			{
				Capability: 'FactoDeploy',
				Name: 'FactoDeployProvider',
				actions:
				{
					'DeploySchema':
					{
						Description: 'Deploy a dataset schema to a target store connection',
						SettingsSchema:
						[
							{ Name: 'IDDataset', DataType: 'Integer', Required: true },
							{ Name: 'IDStoreConnection', DataType: 'Integer', Required: true },
							{ Name: 'TargetTableName', DataType: 'String' },
						],
						Handler: function (pWorkItem, pContext, fHandlerCallback)
						{
							let tmpSettings = pWorkItem.Settings || {};
							let tmpIDDataset = parseInt(tmpSettings.IDDataset, 10);
							let tmpIDStoreConnection = parseInt(tmpSettings.IDStoreConnection, 10);
							let tmpTargetTableName = tmpSettings.TargetTableName || '';

							// Use the ProjectionEngine's deploySchema method
							let tmpProjectionEngine = null;
							if (tmpFactoFable.servicesMap && tmpFactoFable.servicesMap.RetoldFactoProjectionEngine)
							{
								tmpProjectionEngine = Object.values(tmpFactoFable.servicesMap.RetoldFactoProjectionEngine)[0];
							}

							if (!tmpProjectionEngine)
							{
								return fHandlerCallback(new Error('ProjectionEngine service not available'));
							}

							tmpProjectionEngine.deploySchema(tmpIDDataset, tmpIDStoreConnection, tmpTargetTableName,
								(pError, pResult) =>
								{
									if (pError)
									{
										return fHandlerCallback(pError);
									}
									return fHandlerCallback(null, { Outputs: { DeployResult: pResult } });
								});
						}
					}
				}
			});

		this._factoBeacon.enable(
			(pError, pBeaconInfo) =>
			{
				if (pError)
				{
					this.fable.log.error(`[ServerManager] Facto beacon registration failed: ${pError.message}`);
					// Non-fatal — continue without beacon
					return fCallback(null);
				}
				this.fable.log.info(`[ServerManager] Facto registered as beacon: ${pBeaconInfo.BeaconID}`);
				return fCallback(null);
			});
	}

	// ─────────────────────────────────────────────
	//  Meadow-Integration beacon
	// ─────────────────────────────────────────────

	_registerIntegrationBeacon(fCallback)
	{
		let tmpIntegrationFable = this._integrationFable;

		if (!tmpIntegrationFable)
		{
			return fCallback(new Error('Integration fable not available'));
		}

		tmpIntegrationFable.serviceManager.addServiceTypeIfNotExists('UltravisorBeacon', libBeaconService);
		let tmpBeacon = tmpIntegrationFable.instantiateServiceProviderWithoutRegistration(
			'UltravisorBeacon',
			{
				ServerURL:        `http://localhost:${ULTRAVISOR_PORT}`,
				Name:             'meadow-integration',
				MaxConcurrent:    5,
				PollIntervalMs:   2000,
				HeartbeatIntervalMs: 30000,
			});

		this._integrationBeacon = tmpBeacon;

		let tmpLog = this.fable.log;

		tmpBeacon.registerCapability(
			{
				Capability: 'MeadowIntegration',
				Name:       'MeadowIntegrationProvider',
				actions:
				{
					'ParseContent':
					{
						Description: 'Parse raw content (CSV, JSON, XML, etc.) into records',
						SettingsSchema:
						[
							{ Name: 'Content', DataType: 'String', Required: true },
							{ Name: 'Format', DataType: 'String' },
						],
						Handler: function (pWorkItem, pContext, fHandlerCallback)
						{
							let tmpSettings = pWorkItem.Settings || {};
							let tmpContent = tmpSettings.Content || '';
							let tmpOptions = {};
							if (tmpSettings.Format)
							{
								tmpOptions.format = tmpSettings.Format;
							}

							tmpIntegrationFable.MeadowIntegrationFileParser.parseContent(
								tmpContent, tmpOptions,
								(pError, pRecords) =>
								{
									if (pError)
									{
										return fHandlerCallback(pError);
									}
									return fHandlerCallback(null, { Outputs: { Records: pRecords || [], Count: (pRecords || []).length } });
								});
						}
					},
					'TransformRecords':
					{
						Description: 'Apply a mapping configuration to records via TabularTransform',
						SettingsSchema:
						[
							{ Name: 'Records', DataType: 'Array', Required: true },
							{ Name: 'MappingConfiguration', DataType: 'Object', Required: true },
							{ Name: 'EntityName', DataType: 'String', Required: true },
						],
						Handler: function (pWorkItem, pContext, fHandlerCallback)
						{
							let tmpSettings = pWorkItem.Settings || {};
							let tmpRecords = tmpSettings.Records || [];
							let tmpMappingConfig = tmpSettings.MappingConfiguration || {};
							let tmpEntityName = tmpSettings.EntityName || tmpMappingConfig.Entity || 'Unknown';

							let tmpMappingOutcome =
							{
								Configuration: {},
								ImplicitConfiguration: { Entity: tmpEntityName, Mappings: {} },
								ExplicitConfiguration: tmpMappingConfig,
								Comprehension: {},
							};

							try
							{
								tmpIntegrationFable.TabularTransform.initializeMappingOutcomeObject(tmpMappingOutcome);

								for (let i = 0; i < tmpRecords.length; i++)
								{
									tmpIntegrationFable.TabularTransform.transformRecord(tmpRecords[i], tmpMappingOutcome);
								}

								let tmpComprehension = tmpMappingOutcome.Comprehension[tmpEntityName] || {};
								let tmpFlatRecords = Object.values(tmpComprehension);

								return fHandlerCallback(null,
								{
									Outputs:
									{
										Records:      tmpFlatRecords,
										Count:        tmpFlatRecords.length,
										ParsedCount:  tmpMappingOutcome.ParsedRowCount || tmpRecords.length,
										BadRecords:   (tmpMappingOutcome.BadRecords || []).length,
										Entity:       tmpEntityName,
									}
								});
							}
							catch (pTransformError)
							{
								return fHandlerCallback(pTransformError);
							}
						}
					},
					'ParseFile':
					{
						Description: 'Parse a file from a path into records',
						SettingsSchema:
						[
							{ Name: 'FilePath', DataType: 'String', Required: true },
							{ Name: 'Format', DataType: 'String' },
						],
						Handler: function (pWorkItem, pContext, fHandlerCallback)
						{
							let tmpSettings = pWorkItem.Settings || {};
							let tmpFilePath = tmpSettings.FilePath || '';
							let tmpOptions = {};
							if (tmpSettings.Format)
							{
								tmpOptions.format = tmpSettings.Format;
							}

							let tmpAllRecords = [];
							tmpIntegrationFable.MeadowIntegrationFileParser.parseFile(
								tmpFilePath, tmpOptions,
								(pChunkError, pChunkRecords) =>
								{
									if (!pChunkError && Array.isArray(pChunkRecords))
									{
										for (let i = 0; i < pChunkRecords.length; i++)
										{
											tmpAllRecords.push(pChunkRecords[i]);
										}
									}
								},
								(pError, pTotalCount) =>
								{
									if (pError)
									{
										return fHandlerCallback(pError);
									}
									return fHandlerCallback(null, { Outputs: { Records: tmpAllRecords, Count: tmpAllRecords.length } });
								});
						}
					},
				}
			});

		tmpBeacon.enable(
			(pError, pBeaconInfo) =>
			{
				if (pError)
				{
					tmpLog.error(`[ServerManager] Integration beacon registration failed: ${pError.message}`);
					return fCallback(null);
				}
				tmpLog.info(`[ServerManager] Meadow-Integration registered as beacon: ${pBeaconInfo.BeaconID}`);
				return fCallback(null);
			});
	}

	// ─────────────────────────────────────────────
	//  Public API
	// ─────────────────────────────────────────────

	startAll(pDataDir, fCallback)
	{
		this._startFactoServer(pDataDir,
			(pFactoError) =>
			{
				if (pFactoError)
				{
					return fCallback(pFactoError);
				}
				this._startIntegrationServer(pDataDir,
					(pIntegrationError) =>
					{
						if (pIntegrationError)
						{
							return fCallback(pIntegrationError);
						}
						this._startUltravisorServer(pDataDir,
							(pUltravisorError) =>
							{
								if (pUltravisorError)
								{
									return fCallback(pUltravisorError);
								}
								// After all servers are up, register both beacons with Ultravisor
								this._registerFactoBeacon(
									() =>
									{
										this._registerIntegrationBeacon(fCallback);
									});
							});
					});
			});
	}

	stopAll(fCallback)
	{
		// Disable beacons first so they stop WebSocket/polling
		if (this._factoBeacon && typeof this._factoBeacon.disable === 'function')
		{
			try { this._factoBeacon.disable(() => {}); }
			catch (pBeaconErr) { /* non-fatal */ }
		}
		this._factoBeacon = null;

		if (this._integrationBeacon && typeof this._integrationBeacon.disable === 'function')
		{
			try { this._integrationBeacon.disable(() => {}); }
			catch (pBeaconErr) { /* non-fatal */ }
		}
		this._integrationBeacon = null;

		this._stopFactoServer(
			() =>
			{
				this._stopIntegrationServer(
					() =>
					{
						this._stopUltravisorServer(fCallback);
					});
			});
	}

	restartAll(pDataDir, fCallback)
	{
		this.stopAll(
			() =>
			{
				// Brief pause to let OS release ports before rebinding
				setTimeout(
					() =>
					{
						this.startAll(pDataDir, fCallback);
					}, 300);
			});
	}
}

module.exports = ServiceServerManager;
