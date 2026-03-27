/**
 * Service-TestOrchestrator
 *
 * Orchestrates the end-to-end integration test pipeline.  Data flows
 * through Ultravisor operations dispatched to the Facto beacon.
 *
 * Pipeline per dataset:
 *   1. Scan   — locate data file in facto-library (in-process)
 *   2. Parse  — FileParser (in-process — streaming parse)
 *   3. Ingest — trigger Ultravisor facto-ingest operation which dispatches
 *               to the Facto beacon: CreateSource, CreateDataset,
 *               CreateIngestJob, BulkCreateRecords, UpdateIngestJob
 *   4. Verify — query Facto's record count by IDDataset to confirm data landed
 */
'use strict';

const libFableServiceProviderBase = require('fable-serviceproviderbase');
const libFs = require('fs');
const libPath = require('path');
const libHttp = require('http');
const libMeadowIntegration = require('meadow-integration');

// ── Constants ───────────────────────────────────────────────────────────────────

const FACTO_BASE      = 'http://localhost:8420/1.0/';
const ULTRAVISOR_BASE = 'http://localhost:8422/';

// Cap per dataset — keeps the harness snappy while still exercising the stack.
const MAX_RECORDS_PER_DATASET = Infinity;

// Known dataset configs
const DATASET_REGISTRY =
{
	'datahub-country-codes':
	{
		files: ['data/country-codes.csv'],
		format: 'csv',
	},
	'datahub-currency-codes':
	{
		files: ['data/codes-all.csv'],
		format: 'csv',
	},
	'datahub-language-codes':
	{
		files: ['data/language-codes-full.csv'],
		format: 'csv',
	},
	'iana-tlds':
	{
		files: ['data/tlds-alpha-by-domain.txt'],
		format: 'csv',
		filterComments: true,
	},
	'ral-colors':
	{
		files: ['data/raw'],
		format: 'csv',
	},
	'debian-iso-codes':
	{
		files: ['data/languages.csv', 'data/countries.csv'],
		format: 'csv',
	},
	// ── Additional datasets (10 new) ────────────────────────────────────
	'bls-sic-titles':
	{
		files: ['data/sic_industry_titles.csv'],
		format: 'csv',
	},
	'bls-soc-2018':
	{
		files: ['data/2019_Occupations.csv'],
		format: 'csv',
	},
	'iso-10383-mic-codes':
	{
		files: ['data/ISO10383_MIC.csv'],
		format: 'csv',
	},
	'ipeds':
	{
		files: ['data/HD2023.csv'],
		format: 'csv',
	},
	'nflverse':
	{
		files: ['data/combine.csv'],
		format: 'csv',
	},
	'ieee-oui':
	{
		files: ['data/oui.csv'],
		format: 'csv',
	},
	'ourairports':
	{
		files: ['data/airports.csv'],
		format: 'csv',
	},
	'tiger-relationship-files':
	{
		files: ['data/2020_Census_Tract_to_2020_PUMA.txt'],
		format: 'csv',
	},
	'project-gutenberg-catalog':
	{
		files: ['data/pg_catalog.csv'],
		format: 'csv',
	},
	'pantheon':
	{
		files: ['data/person_2020.csv'],
		format: 'csv',
	},
	// ── Bookstore: multi-entity extraction via TabularTransform ──────
	'bookstore':
	{
		files: ['books.csv'],
		format: 'csv',
		fixtureSource: true,
		mappings:
		[
			{ file: 'bookstore/mapping_books_book.json', entity: 'Book' },
			{ file: 'bookstore/mapping_books_author.json', entity: 'Author' },
			{ file: 'bookstore/mapping_books_BookAuthorJoin.json', entity: 'BookAuthorJoin' },
		],
	},
};

// ── ServiceTestOrchestrator ─────────────────────────────────────────────────────

class ServiceTestOrchestrator extends libFableServiceProviderBase
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);

		this.serviceType = 'HarnessTestOrchestrator';

		// File parser (in-process — streaming parse is too valuable to give up)
		this.fable.addAndInstantiateServiceTypeIfNotExists(
			'MeadowIntegrationFileParser',
			libMeadowIntegration.FileParser
		);
	}

	// ─────────────────────────────────────────────
	//  Public: runSuite
	// ─────────────────────────────────────────────

	runSuite(pDatasets, pFactoLibraryPath, pDataDir, fProgress, fCallback)
	{
		// Verify servers are reachable before starting
		let tmpServerManager = this.fable.HarnessServerManager;
		if (!tmpServerManager || !tmpServerManager.factoRunning)
		{
			return fCallback(new Error(
				'Servers are not running — use "Clean & Execute" to start them first'
			));
		}

		let tmpResults = [];
		let tmpIndex   = 0;

		// Start throughput tracking
		this._startThroughputRun(`Suite: ${pDatasets.length} datasets`);

		const processNext = () =>
		{
			if (tmpIndex >= pDatasets.length)
			{
				this._endThroughputRun();
				return fCallback(null, tmpResults);
			}

			let tmpDatasetName = pDatasets[tmpIndex++];
			let tmpResult =
			{
				dataset:  tmpDatasetName,
				status:   'pending',
				parsed:   0,
				loaded:   0,
				verified: 0,
				error:    null,
			};
			tmpResults.push(tmpResult);

			fProgress(`[${tmpIndex}/${pDatasets.length}] ${tmpDatasetName} — scanning...`);

			// ── 1. Scan ──────────────────────────────────────────────────────────
			let tmpConfig      = DATASET_REGISTRY[tmpDatasetName];
			let tmpDatasetRoot = tmpConfig && tmpConfig.fixtureSource
				? libPath.resolve(__dirname, '..', '..', 'fixtures')
				: libPath.join(pFactoLibraryPath, tmpDatasetName);
			let tmpFilePath    = null;

			if (tmpConfig)
			{
				for (let tmpRelPath of tmpConfig.files)
				{
					let tmpCandidate = libPath.join(tmpDatasetRoot, tmpRelPath);
					if (libFs.existsSync(tmpCandidate))
					{
						tmpFilePath = tmpCandidate;
						break;
					}
				}
			}
			else
			{
				tmpFilePath = this._autoDiscover(tmpDatasetRoot);
			}

			if (!tmpFilePath)
			{
				tmpResult.status = 'skip';
				tmpResult.error  = 'No data file found in facto-library';
				fProgress(`[SKIP] ${tmpDatasetName} — no data file found`);
				return setImmediate(processNext);
			}

			fProgress(`[${tmpIndex}/${pDatasets.length}] ${tmpDatasetName} — parsing ${libPath.basename(tmpFilePath)}...`);

			// ── 2. Parse ─────────────────────────────────────────────────────────
			this._parseFile(tmpFilePath, tmpConfig || {},
				(pParseError, pRecords) =>
				{
					if (pParseError)
					{
						tmpResult.status = 'fail';
						tmpResult.error  = `Parse error: ${pParseError.message}`;
						fProgress(`[FAIL] ${tmpDatasetName} — ${tmpResult.error}`);
						return setImmediate(processNext);
					}

					tmpResult.parsed = pRecords.length;

					if (tmpResult.parsed === 0)
					{
						tmpResult.status = 'fail';
						tmpResult.error  = 'No records parsed from file';
						fProgress(`[FAIL] ${tmpDatasetName} — 0 records parsed`);
						return setImmediate(processNext);
					}

					// Emit "extracted" throughput event
					this._emitThroughput('extracted', pRecords.length, tmpDatasetName);

					// ── Multi-entity mapping path ────────────────────────────
					if (tmpConfig && tmpConfig.mappings)
					{
						this._runMappedIngest(tmpDatasetName, pRecords, tmpConfig, tmpIndex, pDatasets.length,
							fProgress, tmpResult, tmpResults,
							() => { return setImmediate(processNext); });
						return;
					}

					// ── Standard identity ingest ─────────────────────────────
					let tmpTransformed = this._applyTransform(pRecords, tmpDatasetName);

					// Emit "transformed" throughput event (identity transform)
					this._emitThroughput('transformed', tmpTransformed.length, tmpDatasetName);

					fProgress(`[${tmpIndex}/${pDatasets.length}] ${tmpDatasetName} — triggering Ultravisor ingest (${tmpTransformed.length} records)...`);

					this._triggerUltravisorIngest(tmpDatasetName, tmpTransformed,
						(pIngestError, pLoadedCount, pIDDataset) =>
						{
							if (pIngestError)
							{
								tmpResult.status = 'fail';
								tmpResult.error  = `Ultravisor ingest: ${pIngestError.message}`;
								fProgress(`[FAIL] ${tmpDatasetName} — ${tmpResult.error}`);
								return setImmediate(processNext);
							}

							tmpResult.loaded = pLoadedCount;

							fProgress(`[${tmpIndex}/${pDatasets.length}] ${tmpDatasetName} — verifying records in Facto...`);

							this._verifyFactoCount(pIDDataset,
								(pVerifyError, pCount) =>
								{
									if (pVerifyError)
									{
										tmpResult.status = 'fail';
										tmpResult.error  = `Verify: ${pVerifyError.message}`;
										fProgress(`[FAIL] ${tmpDatasetName} — ${tmpResult.error}`);
									}
									else
									{
										tmpResult.verified = pCount;
										tmpResult.status   = (tmpResult.verified >= tmpResult.loaded) ? 'pass' : 'fail';

										if (tmpResult.status === 'pass')
										{
											fProgress(`[PASS] ${tmpDatasetName} — verified ${tmpResult.verified} records`);
										}
										else
										{
											tmpResult.error = `Count mismatch: loaded ${tmpResult.loaded}, verified ${tmpResult.verified}`;
											fProgress(`[FAIL] ${tmpDatasetName} — ${tmpResult.error}`);
										}
									}

									return setImmediate(processNext);
								});
						});
				});
		};

		processNext();
	}

	// ─────────────────────────────────────────────
	//  Multi-entity mapped ingest
	// ─────────────────────────────────────────────

	/**
	 * Run a multi-entity ingest using TabularTransform.
	 * For each mapping config, transforms the parsed records into a
	 * comprehension, flattens it, and ingests as a separate Facto dataset.
	 *
	 * Creates one result entry per entity in the results array.
	 */
	_runMappedIngest(pDatasetName, pRecords, pConfig, pIndex, pTotal, fProgress, pParentResult, pResults, fDone)
	{
		let tmpFixtureDir = libPath.resolve(__dirname, '..', '..', 'fixtures');
		let tmpMappings = pConfig.mappings;
		let tmpTabularTransform = this.fable.HarnessServerManager._factoFable.TabularTransform;

		if (!tmpTabularTransform)
		{
			pParentResult.status = 'fail';
			pParentResult.error  = 'TabularTransform not available on Facto fable instance';
			fProgress(`[FAIL] ${pDatasetName} — TabularTransform not found`);
			return fDone();
		}

		// Mark parent result as a container
		pParentResult.status = 'pass';
		pParentResult.parsed = pRecords.length;

		let tmpMappingIndex = 0;

		const processNextMapping = () =>
		{
			if (tmpMappingIndex >= tmpMappings.length)
			{
				return fDone();
			}

			let tmpMapping = tmpMappings[tmpMappingIndex++];
			let tmpEntityName = tmpMapping.entity;
			let tmpDatasetFullName = `${pDatasetName}-${tmpEntityName}`;

			// Create a sub-result for this entity
			let tmpSubResult =
			{
				dataset:  tmpDatasetFullName,
				status:   'pending',
				parsed:   pRecords.length,
				loaded:   0,
				verified: 0,
				error:    null,
			};
			pResults.push(tmpSubResult);

			fProgress(`[${pIndex}/${pTotal}] ${pDatasetName} — transforming → ${tmpEntityName}...`);

			// Load the mapping configuration
			let tmpMappingConfig;
			try
			{
				tmpMappingConfig = JSON.parse(
					libFs.readFileSync(libPath.join(tmpFixtureDir, tmpMapping.file), 'utf8'));
			}
			catch (pLoadError)
			{
				tmpSubResult.status = 'fail';
				tmpSubResult.error  = `Mapping load: ${pLoadError.message}`;
				fProgress(`[FAIL] ${tmpDatasetFullName} — ${tmpSubResult.error}`);
				return setImmediate(processNextMapping);
			}

			// Build MappingOutcome and transform all records.
			// Pre-set ImplicitConfiguration to avoid the reference error
			// in initializeMappingOutcomeObject when no incoming record
			// is available yet.
			let tmpMappingOutcome =
			{
				Configuration: {},
				ImplicitConfiguration: { Entity: tmpEntityName, Mappings: {} },
				ExplicitConfiguration: tmpMappingConfig,
				Comprehension: {},
			};
			tmpTabularTransform.initializeMappingOutcomeObject(tmpMappingOutcome);

			for (let i = 0; i < pRecords.length; i++)
			{
				tmpTabularTransform.transformRecord(pRecords[i], tmpMappingOutcome);
			}

			// Extract the comprehension for this entity
			let tmpComprehension = tmpMappingOutcome.Comprehension[tmpEntityName] || {};
			let tmpFlatRecords = Object.values(tmpComprehension);

			// Emit "transformed" throughput event
			this._emitThroughput('transformed', tmpFlatRecords.length, tmpDatasetFullName);

			if (tmpFlatRecords.length === 0)
			{
				tmpSubResult.status = 'fail';
				tmpSubResult.error  = `Transform produced 0 ${tmpEntityName} records`;
				fProgress(`[FAIL] ${tmpDatasetFullName} — 0 records after transform`);
				return setImmediate(processNextMapping);
			}

			fProgress(`[${pIndex}/${pTotal}] ${pDatasetName} — ingesting ${tmpFlatRecords.length} ${tmpEntityName} records...`);

			// Wrap each mapped record as a Facto Record with Content
			let tmpFactoRecords = tmpFlatRecords.map(
				(pRecord) =>
				({
					Type:       tmpEntityName,
					IngestDate: new Date().toISOString(),
					Version:    1,
					Content:    JSON.stringify(pRecord),
				}));

			// Trigger Ultravisor ingest for this entity's dataset
			this._triggerUltravisorIngest(tmpDatasetFullName, tmpFactoRecords,
				(pIngestError, pLoadedCount, pIDDataset) =>
				{
					if (pIngestError)
					{
						tmpSubResult.status = 'fail';
						tmpSubResult.error  = `Ingest: ${pIngestError.message}`;
						fProgress(`[FAIL] ${tmpDatasetFullName} — ${tmpSubResult.error}`);
						return setImmediate(processNextMapping);
					}

					tmpSubResult.loaded = pLoadedCount;

					// Verify in Facto
					this._verifyFactoCount(pIDDataset,
						(pVerifyError, pCount) =>
						{
							if (pVerifyError)
							{
								tmpSubResult.status = 'fail';
								tmpSubResult.error  = `Verify: ${pVerifyError.message}`;
								fProgress(`[FAIL] ${tmpDatasetFullName} — ${tmpSubResult.error}`);
							}
							else
							{
								tmpSubResult.verified = pCount;
								tmpSubResult.status   = (tmpSubResult.verified >= tmpSubResult.loaded) ? 'pass' : 'fail';

								if (tmpSubResult.status === 'pass')
								{
									fProgress(`[PASS] ${tmpDatasetFullName} — verified ${tmpSubResult.verified} records`);
								}
								else
								{
									tmpSubResult.error = `Count mismatch: loaded=${tmpSubResult.loaded} verified=${tmpSubResult.verified}`;
									fProgress(`[FAIL] ${tmpDatasetFullName} — ${tmpSubResult.error}`);
								}
							}

							return setImmediate(processNextMapping);
						});
				});
		};

		processNextMapping();
	}

	// ─────────────────────────────────────────────
	//  HTTP helpers
	// ─────────────────────────────────────────────

	/**
	 * POST a JSON body to pUrl.  Calls fCallback(err, parsedBody, statusCode).
	 */
	_httpPost(pUrl, pBody, fCallback)
	{
		let tmpBody    = JSON.stringify(pBody);
		let tmpParsed  = new URL(pUrl);

		let tmpOptions =
		{
			hostname: tmpParsed.hostname,
			port:     parseInt(tmpParsed.port, 10),
			path:     tmpParsed.pathname + (tmpParsed.search || ''),
			method:   'POST',
			headers:
			{
				'Content-Type':   'application/json',
				'Content-Length': Buffer.byteLength(tmpBody),
			},
		};

		let tmpReq = libHttp.request(tmpOptions,
			(pRes) =>
			{
				let tmpData = '';
				pRes.on('data', (pChunk) => { tmpData += pChunk; });
				pRes.on('end',
					() =>
					{
						try
						{
							return fCallback(null, JSON.parse(tmpData), pRes.statusCode);
						}
						catch (pParseError)
						{
							return fCallback(null, tmpData, pRes.statusCode);
						}
					});
			});

		tmpReq.on('error', fCallback);
		tmpReq.write(tmpBody);
		tmpReq.end();
	}

	/**
	 * PUT a JSON body to pUrl.  Calls fCallback(err, parsedBody, statusCode).
	 */
	_httpPut(pUrl, pBody, fCallback)
	{
		let tmpBody    = JSON.stringify(pBody);
		let tmpParsed  = new URL(pUrl);

		let tmpOptions =
		{
			hostname: tmpParsed.hostname,
			port:     parseInt(tmpParsed.port, 10),
			path:     tmpParsed.pathname + (tmpParsed.search || ''),
			method:   'PUT',
			headers:
			{
				'Content-Type':   'application/json',
				'Content-Length': Buffer.byteLength(tmpBody),
			},
		};

		let tmpReq = libHttp.request(tmpOptions,
			(pRes) =>
			{
				let tmpData = '';
				pRes.on('data', (pChunk) => { tmpData += pChunk; });
				pRes.on('end',
					() =>
					{
						try
						{
							return fCallback(null, JSON.parse(tmpData), pRes.statusCode);
						}
						catch (pParseError)
						{
							return fCallback(null, tmpData, pRes.statusCode);
						}
					});
			});

		tmpReq.on('error', fCallback);
		tmpReq.write(tmpBody);
		tmpReq.end();
	}

	/**
	 * GET from pUrl.  Calls fCallback(err, parsedBody, statusCode).
	 */
	_httpGet(pUrl, fCallback)
	{
		let tmpParsed  = new URL(pUrl);
		let tmpOptions =
		{
			hostname: tmpParsed.hostname,
			port:     parseInt(tmpParsed.port, 10),
			path:     tmpParsed.pathname + (tmpParsed.search || ''),
			method:   'GET',
		};

		let tmpReq = libHttp.request(tmpOptions,
			(pRes) =>
			{
				let tmpData = '';
				pRes.on('data', (pChunk) => { tmpData += pChunk; });
				pRes.on('end',
					() =>
					{
						try
						{
							return fCallback(null, JSON.parse(tmpData), pRes.statusCode);
						}
						catch (pParseError)
						{
							return fCallback(null, tmpData, pRes.statusCode);
						}
					});
			});

		tmpReq.on('error', fCallback);
		tmpReq.end();
	}

	// ─────────────────────────────────────────────
	//  Pipeline steps
	// ─────────────────────────────────────────────

	/**
	 * Trigger the facto-ingest operation on Ultravisor.
	 * The operation dispatches work to facto's beacon (FactoData capability)
	 * to create Source, Dataset, IngestJob, and bulk-insert Records.
	 *
	 * @param {string} pDatasetName — dataset identifier
	 * @param {Array} pRecords — pre-parsed and transformed records
	 * @param {Function} fCallback — function(pError, pLoadedCount)
	 */
	_triggerUltravisorIngest(pDatasetName, pRecords, fCallback)
	{
		// If records already have Content (from mapped ingest), pass through.
		// Otherwise wrap raw parsed objects into the Facto Record envelope.
		let tmpRecordContents = pRecords.map(
			(pRecord) =>
			{
				if (pRecord.Content !== undefined)
				{
					return pRecord;
				}
				return {
					Type:       'HarnessRecord',
					IngestDate: new Date().toISOString(),
					Version:    1,
					Content:    JSON.stringify(pRecord),
				};
			});

		this._httpPost(`${ULTRAVISOR_BASE}Operation/facto-ingest/Trigger`,
			{
				Parameters:
				{
					DatasetName: pDatasetName,
					Records:     tmpRecordContents,
				},
				Async: false,
				TimeoutMs: 120000,
			},
			(pError, pBody, pStatus) =>
			{
				if (pError)
				{
					return fCallback(new Error(`Ultravisor trigger failed: ${pError.message}`));
				}
				if (!pBody || !pBody.Success)
				{
					let tmpMsg = (pBody && pBody.Errors && pBody.Errors.length > 0)
						? pBody.Errors.map((e) => e.Message || e).join('; ')
						: `HTTP ${pStatus}`;
					return fCallback(new Error(`Ultravisor ingest failed: ${tmpMsg}`));
				}

				// Extract loaded count and IDDataset from operation output
				let tmpLoadedCount = 0;
				let tmpIDDataset = 0;
				if (pBody.TaskOutputs)
				{
					let tmpOutputKeys = Object.keys(pBody.TaskOutputs);
					for (let tmpKey of tmpOutputKeys)
					{
						let tmpOut = pBody.TaskOutputs[tmpKey];
						if (tmpOut && typeof tmpOut.Count === 'number')
						{
							tmpLoadedCount = tmpOut.Count;
						}
						if (tmpOut && tmpOut.Created && tmpOut.Created.IDDataset)
						{
							tmpIDDataset = tmpOut.Created.IDDataset;
						}
					}
				}

				// Fall back to input count if output not found
				if (tmpLoadedCount === 0)
				{
					tmpLoadedCount = pRecords.length;
				}

				return fCallback(null, tmpLoadedCount, tmpIDDataset);
			});
	}

	/**
	 * Create Source, Dataset, IngestJob in Facto via HTTP.
	 * Returns IDs via fCallback(err, IDSource, IDDataset, IDIngestJob).
	 */
	_createFactoEntities(pDatasetName, fCallback)
	{
		// 1 — Source
		this._httpPost(`${FACTO_BASE}Source`,
			{ Name: pDatasetName, Type: 'HarnessDataset', Active: 1 },
			(pSourceError, pSourceBody, pSourceStatus) =>
			{
				if (pSourceError)
				{
					return fCallback(new Error(`POST Source failed: ${pSourceError.message}`));
				}
				if (pSourceStatus !== 200)
				{
					return fCallback(new Error(`POST Source HTTP ${pSourceStatus}`));
				}

				let tmpIDSource = pSourceBody && pSourceBody.IDSource ? pSourceBody.IDSource : 0;

				// 2 — Dataset
				this._httpPost(`${FACTO_BASE}Dataset`,
					{ Name: pDatasetName, Type: 'HarnessIngest' },
					(pDatasetError, pDatasetBody, pDatasetStatus) =>
					{
						if (pDatasetError)
						{
							return fCallback(new Error(`POST Dataset failed: ${pDatasetError.message}`));
						}
						if (pDatasetStatus !== 200)
						{
							return fCallback(new Error(`POST Dataset HTTP ${pDatasetStatus}`));
						}

						let tmpIDDataset = pDatasetBody && pDatasetBody.IDDataset ? pDatasetBody.IDDataset : 0;

						// 3 — IngestJob
						this._httpPost(`${FACTO_BASE}IngestJob`,
							{
								IDSource:  tmpIDSource,
								IDDataset: tmpIDDataset,
								Status:    'Running',
								StartDate: new Date().toISOString(),
							},
							(pJobError, pJobBody, pJobStatus) =>
							{
								if (pJobError)
								{
									return fCallback(new Error(`POST IngestJob failed: ${pJobError.message}`));
								}
								if (pJobStatus !== 200)
								{
									return fCallback(new Error(`POST IngestJob HTTP ${pJobStatus}`));
								}

								let tmpIDIngestJob = pJobBody && pJobBody.IDIngestJob ? pJobBody.IDIngestJob : 0;

								return fCallback(null, tmpIDSource, tmpIDDataset, tmpIDIngestJob);
							});
					});
			});
	}

	/**
	 * POST transformed records individually to Facto's /1.0/Record endpoint.
	 * Returns fCallback(err, loadedCount).
	 */
	_postRecordsToFacto(pRecords, pIDDataset, pIDSource, pIDIngestJob, fCallback)
	{
		let tmpLoaded   = 0;
		let tmpIndex    = 0;
		let tmpNow      = new Date().toISOString();

		const postNext = () =>
		{
			if (tmpIndex >= pRecords.length)
			{
				return fCallback(null, tmpLoaded);
			}

			let tmpRecord = pRecords[tmpIndex++];

			this._httpPost(`${FACTO_BASE}Record`,
				{
					IDDataset:   pIDDataset,
					IDSource:    pIDSource,
					IDIngestJob: pIDIngestJob,
					Type:        'HarnessRecord',
					IngestDate:  tmpNow,
					Version:     1,
					Content:     JSON.stringify(tmpRecord),
				},
				(pError, pBody, pStatus) =>
				{
					if (!pError && pStatus === 200)
					{
						tmpLoaded++;
					}
					return setImmediate(postNext);
				});
		};

		postNext();
	}

	/**
	 * Use meadow-integration IntegrationAdapter + REST client to push records
	 * to the integration target server on port 8421.
	 */
	/**
	 * Verify the record count in Facto for a given dataset.
	 * Queries Facto's record count by IDDataset via the REST API.
	 */
	_verifyFactoCount(pIDDataset, fCallback)
	{
		if (!pIDDataset)
		{
			return fCallback(new Error('No IDDataset returned from ingest operation'));
		}
		this._httpGet(
			`${FACTO_BASE}Records/Count/By/IDDataset/${pIDDataset}`,
			(pError, pBody, pStatus) =>
			{
				if (pError)
				{
					return fCallback(new Error(`Facto verify GET failed: ${pError.message}`));
				}
				if (pStatus !== 200)
				{
					return fCallback(new Error(`Facto verify HTTP ${pStatus}`));
				}
				// meadow-endpoints Count returns { Count: N }
				let tmpCount = pBody && typeof pBody.Count === 'number'
					? pBody.Count
					: (typeof pBody === 'number' ? pBody : 0);
				return fCallback(null, tmpCount);
			});
	}

	/**
	 * Update the IngestJob record to reflect the final run status.
	 */
	_finalizeIngestJob(pIDIngestJob, pStatus, pProcessed, pCreated, fCallback)
	{
		if (!pIDIngestJob)
		{
			return fCallback(null);
		}

		this._httpPut(`${FACTO_BASE}IngestJob`,
			{
				IDIngestJob:       pIDIngestJob,
				Status:            pStatus,
				EndDate:           new Date().toISOString(),
				RecordsProcessed:  pProcessed,
				RecordsCreated:    pCreated,
			},
			() => { return fCallback(null); });
	}

	// ─────────────────────────────────────────────
	//  Throughput instrumentation
	// ─────────────────────────────────────────────

	/**
	 * Emit a throughput event to Facto's ThroughputMonitor.
	 * Uses the in-process service if available, falls back to HTTP POST.
	 */
	_emitThroughput(pStage, pCount, pDatasetName)
	{
		// Fast path: use in-process ThroughputMonitor directly
		let tmpServerManager = this.fable.HarnessServerManager;
		if (tmpServerManager && tmpServerManager._factoFable && tmpServerManager._factoFable.ThroughputMonitor)
		{
			tmpServerManager._factoFable.ThroughputMonitor.recordEvent(pStage, pCount, pDatasetName);
			return;
		}

		// Fallback: POST to the REST endpoint (fire-and-forget)
		this._httpPost(`${FACTO_BASE}facto/throughput/event`,
			{ Stage: pStage, Count: pCount, Dataset: pDatasetName },
			() => {});
	}

	/**
	 * Signal the start of a pipeline run to the ThroughputMonitor.
	 */
	_startThroughputRun(pLabel)
	{
		let tmpServerManager = this.fable.HarnessServerManager;
		if (tmpServerManager && tmpServerManager._factoFable && tmpServerManager._factoFable.ThroughputMonitor)
		{
			tmpServerManager._factoFable.ThroughputMonitor.startRun(pLabel);
			return;
		}
		this._httpPost(`${FACTO_BASE}facto/throughput/run/start`, { Label: pLabel }, () => {});
	}

	/**
	 * Signal the end of a pipeline run.
	 */
	_endThroughputRun()
	{
		let tmpServerManager = this.fable.HarnessServerManager;
		if (tmpServerManager && tmpServerManager._factoFable && tmpServerManager._factoFable.ThroughputMonitor)
		{
			tmpServerManager._factoFable.ThroughputMonitor.endRun();
			return;
		}
		this._httpPost(`${FACTO_BASE}facto/throughput/run/end`, {}, () => {});
	}

	// ─────────────────────────────────────────────
	//  In-process helpers (scan, parse, transform)
	// ─────────────────────────────────────────────

	_autoDiscover(pDatasetRoot)
	{
		let tmpDataSubdir = libPath.join(pDatasetRoot, 'data');
		let tmpSearchDir  = libFs.existsSync(tmpDataSubdir) ? tmpDataSubdir : pDatasetRoot;

		if (!libFs.existsSync(tmpSearchDir))
		{
			return null;
		}

		let tmpEntries;
		try
		{
			tmpEntries = libFs.readdirSync(tmpSearchDir);
		}
		catch (pError)
		{
			return null;
		}

		let tmpExtensions = ['.csv', '.json', '.jsonl', '.tsv', '.txt'];
		for (let tmpEntry of tmpEntries)
		{
			let tmpExt = libPath.extname(tmpEntry).toLowerCase();
			if (tmpExtensions.includes(tmpExt))
			{
				return libPath.join(tmpSearchDir, tmpEntry);
			}
		}

		return null;
	}

	_parseFile(pFilePath, pConfig, fCallback)
	{
		let tmpRecords = [];
		let tmpOptions = {};

		if (pConfig.format)
		{
			tmpOptions.format = pConfig.format;
		}

		if (pConfig.filterComments)
		{
			try
			{
				let tmpRaw      = libFs.readFileSync(pFilePath, 'utf8');
				let tmpFiltered = tmpRaw
					.split('\n')
					.filter((pLine) => !pLine.trim().startsWith('#') && pLine.trim().length > 0)
					.join('\n');

				this.fable.MeadowIntegrationFileParser.parseContent(
					tmpFiltered,
					Object.assign({}, tmpOptions, { format: 'csv' }),
					(pError, pParsed) =>
					{
						if (pError)
						{
							return fCallback(pError);
						}
						return fCallback(null, Array.isArray(pParsed) ? pParsed : []);
					});
			}
			catch (pReadError)
			{
				return fCallback(pReadError);
			}
			return;
		}

		this.fable.MeadowIntegrationFileParser.parseFile(
			pFilePath,
			tmpOptions,
			(pChunkError, pChunkRecords) =>
			{
				if (!pChunkError && Array.isArray(pChunkRecords))
				{
					for (let tmpRecord of pChunkRecords)
					{
						if (tmpRecords.length < MAX_RECORDS_PER_DATASET)
						{
							tmpRecords.push(tmpRecord);
						}
					}
				}
			},
			(pCompletionError) =>
			{
				if (pCompletionError)
				{
					return fCallback(pCompletionError);
				}
				return fCallback(null, tmpRecords);
			});
	}

	_applyTransform(pRecords, pDatasetName)
	{
		let tmpTransformed = [];
		let tmpCount       = Math.min(pRecords.length, MAX_RECORDS_PER_DATASET);

		for (let i = 0; i < tmpCount; i++)
		{
			let tmpRecord = pRecords[i];
			let tmpFlat   = {};

			if (tmpRecord && typeof tmpRecord === 'object')
			{
				for (let tmpKey of Object.keys(tmpRecord))
				{
					let tmpVal = tmpRecord[tmpKey];
					tmpFlat[tmpKey] = (tmpVal !== null && typeof tmpVal === 'object')
						? JSON.stringify(tmpVal)
						: String(tmpVal == null ? '' : tmpVal);
				}
			}
			else if (typeof tmpRecord === 'string')
			{
				tmpFlat.value = tmpRecord;
			}

			tmpTransformed.push(tmpFlat);
		}

		return tmpTransformed;
	}
}

module.exports = ServiceTestOrchestrator;
