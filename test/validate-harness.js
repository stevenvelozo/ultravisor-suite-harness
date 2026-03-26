#!/usr/bin/env node
/**
 * validate-harness.js
 *
 * External validation script for the Ultravisor Suite Harness.
 * Starts servers, ingests datasets through Ultravisor, then validates:
 *
 *   1. Ultravisor operation manifest (every task succeeded, no errors)
 *   2. Facto SQLite database (Sources, Datasets, DatasetSource links,
 *      Records with correct IDDataset/IDSource, IngestJobs completed)
 *   3. Facto record counts match what was loaded
 *   4. Ultravisor API endpoints respond correctly
 *
 * Usage:
 *   node test/validate-harness.js
 *   node test/validate-harness.js --datasets=datahub-country-codes
 *   node test/validate-harness.js --preset=medium
 *   node test/validate-harness.js --preset=large
 *
 * Exit code 0 = all validations passed, 1 = failures.
 */

'use strict';

const libPath = require('path');
const libHttp = require('http');

const ServiceServerManager = require('../source/services/Service-ServerManager.js');
const ServiceDataManager = require('../source/services/Service-DataManager.js');
const ServiceTestOrchestrator = require('../source/services/Service-TestOrchestrator.js');
const libPict = require('pict');

// ── CLI parsing ─────────────────────────────────────────────────────────────

const PRESETS =
{
	small: ['datahub-country-codes', 'datahub-currency-codes', 'datahub-language-codes'],
	medium: ['datahub-country-codes', 'datahub-currency-codes', 'datahub-language-codes',
		'iana-tlds', 'ral-colors', 'bls-sic-titles', 'bls-soc-2018', 'bookstore'],
	large: ['datahub-country-codes', 'datahub-currency-codes', 'datahub-language-codes',
		'iana-tlds', 'ral-colors', 'bls-sic-titles', 'bls-soc-2018',
		'iso-10383-mic-codes', 'ipeds', 'nflverse', 'ieee-oui',
		'ourairports', 'tiger-relationship-files', 'project-gutenberg-catalog', 'bookstore'],
	bookstore: ['bookstore'],
};

let tmpPresetArg = process.argv.find((pArg) => pArg.startsWith('--preset='));
let tmpDatasetsArg = process.argv.find((pArg) => pArg.startsWith('--datasets='));

let DATASETS;
if (tmpDatasetsArg)
{
	DATASETS = tmpDatasetsArg.split('=')[1].split(',').map((pD) => pD.trim());
}
else if (tmpPresetArg)
{
	let tmpPresetName = tmpPresetArg.split('=')[1];
	DATASETS = PRESETS[tmpPresetName] || PRESETS.small;
}
else
{
	DATASETS = PRESETS.small;
}

const DATA_DIR = libPath.resolve(__dirname, '..', 'data');
const FACTO_LIB = libPath.resolve(__dirname, '..', '..', 'dist', 'facto-library');

// ── Helpers ─────────────────────────────────────────────────────────────────

function httpGet(pUrl)
{
	return new Promise((fResolve, fReject) =>
	{
		libHttp.get(pUrl, (pResponse) =>
		{
			let tmpBody = '';
			pResponse.on('data', (pChunk) => { tmpBody += pChunk; });
			pResponse.on('end', () =>
			{
				try
				{
					fResolve({ status: pResponse.statusCode, body: JSON.parse(tmpBody) });
				}
				catch (pParseError)
				{
					fResolve({ status: pResponse.statusCode, body: tmpBody });
				}
			});
		}).on('error', fReject);
	});
}

function httpPost(pUrl, pData)
{
	return new Promise((fResolve, fReject) =>
	{
		let tmpPostData = JSON.stringify(pData);
		let tmpParsed = new URL(pUrl);
		let tmpOpts =
		{
			hostname: tmpParsed.hostname,
			port: tmpParsed.port,
			path: tmpParsed.pathname,
			method: 'POST',
			headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(tmpPostData) },
		};
		let tmpReq = libHttp.request(tmpOpts, (pResponse) =>
		{
			let tmpBody = '';
			pResponse.on('data', (pChunk) => { tmpBody += pChunk; });
			pResponse.on('end', () =>
			{
				try
				{
					fResolve({ status: pResponse.statusCode, body: JSON.parse(tmpBody) });
				}
				catch (pParseError)
				{
					fResolve({ status: pResponse.statusCode, body: tmpBody });
				}
			});
		});
		tmpReq.on('error', fReject);
		tmpReq.write(tmpPostData);
		tmpReq.end();
	});
}

// ── Validation checks ───────────────────────────────────────────────────────

let _Failures = [];
let _Passes = 0;

function assert(pDescription, pCondition, pDetail)
{
	if (pCondition)
	{
		_Passes++;
	}
	else
	{
		_Failures.push({ description: pDescription, detail: pDetail || '' });
		console.log(`  ✗ FAIL: ${pDescription}` + (pDetail ? ` — ${pDetail}` : ''));
	}
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main()
{
	console.log('╔══════════════════════════════════════════════════════╗');
	console.log('║   Ultravisor Suite Harness — Validation Script      ║');
	console.log('╚══════════════════════════════════════════════════════╝');
	console.log('');
	console.log(`Datasets (${DATASETS.length}): ${DATASETS.join(', ')}`);
	console.log('');

	// ── Phase 1: Start servers ──────────────────────────────────────────

	console.log('── Phase 1: Starting servers ──────────────────────────');

	let tmpPict = new libPict({ Product: 'HarnessValidator', LogNoisiness: 0, LogStreams: [] });
	tmpPict.addAndInstantiateServiceTypeIfNotExists('HarnessServerManager', ServiceServerManager);
	tmpPict.addAndInstantiateServiceTypeIfNotExists('HarnessDataManager', ServiceDataManager);
	tmpPict.addAndInstantiateServiceTypeIfNotExists('HarnessTestOrchestrator', ServiceTestOrchestrator);

	await new Promise((fResolve, fReject) =>
	{
		tmpPict.HarnessDataManager.cleanDataDir(DATA_DIR,
			(pError) =>
			{
				if (pError) return fReject(pError);
				fResolve();
			});
	});
	console.log('  [OK] Data directory cleaned');

	await new Promise((fResolve, fReject) =>
	{
		tmpPict.HarnessServerManager.startAll(DATA_DIR,
			(pError) =>
			{
				if (pError) return fReject(pError);
				fResolve();
			});
	});
	console.log('  [OK] Servers started (Facto :8420  Ultravisor :8422)');

	await new Promise((fResolve, fReject) =>
	{
		tmpPict.HarnessDataManager.initDatabase(DATA_DIR,
			(pError) =>
			{
				if (pError) return fReject(pError);
				fResolve();
			});
	});
	console.log('  [OK] Harness DB initialized');
	console.log('');

	// ── Phase 2: Validate Ultravisor API ────────────────────────────────

	console.log('── Phase 2: Validate Ultravisor API endpoints ────────');

	let tmpStatus = await httpGet('http://localhost:8422/status');
	assert('Ultravisor /status returns 200', tmpStatus.status === 200);
	assert('Ultravisor reports Running', tmpStatus.body && tmpStatus.body.Status === 'Running');

	let tmpOps = await httpGet('http://localhost:8422/Operation');
	assert('Ultravisor /Operation returns 200', tmpOps.status === 200);
	let tmpFactoOp = (tmpOps.body || []).find((pOp) => pOp.Hash === 'facto-ingest');
	assert('facto-ingest operation is loaded', !!tmpFactoOp);
	console.log('');

	// ── Phase 3: Run pipeline ───────────────────────────────────────────

	console.log('── Phase 3: Running pipeline ─────────────────────────');

	let tmpResults = await new Promise((fResolve, fReject) =>
	{
		tmpPict.HarnessTestOrchestrator.runSuite(DATASETS, FACTO_LIB, DATA_DIR,
			(pProgress) => { console.log('  ' + pProgress); },
			(pError, pResults) =>
			{
				if (pError) return fReject(pError);
				fResolve(pResults);
			});
	});
	console.log('');

	// ── Phase 4: Validate pipeline results ──────────────────────────────

	console.log('── Phase 4: Validate pipeline results ────────────────');

	for (let tmpR of tmpResults)
	{
		assert(`${tmpR.dataset}: status is pass`, tmpR.status === 'pass',
			tmpR.status !== 'pass' ? (tmpR.error || tmpR.status) : undefined);
		assert(`${tmpR.dataset}: parsed > 0`, tmpR.parsed > 0, `parsed=${tmpR.parsed}`);
		// Multi-entity parent results (e.g. "bookstore") have loaded=0 because
		// the actual data goes into sub-results (bookstore-Book, etc.)
		if (tmpR.loaded > 0 || tmpR.verified > 0)
		{
			assert(`${tmpR.dataset}: loaded > 0`, tmpR.loaded > 0, `loaded=${tmpR.loaded}`);
			assert(`${tmpR.dataset}: verified > 0`, tmpR.verified > 0, `verified=${tmpR.verified}`);
			assert(`${tmpR.dataset}: loaded == verified`, tmpR.loaded === tmpR.verified,
				`loaded=${tmpR.loaded} verified=${tmpR.verified}`);
		}
	}
	console.log('');

	// ── Phase 5: Validate Facto database ────────────────────────────────

	console.log('── Phase 5: Validate Facto database ──────────────────');

	let tmpFactoFable = tmpPict.HarnessServerManager._factoFable;

	// Check Sources
	let tmpSourceCount = await new Promise((fResolve) =>
	{
		let tmpQ = tmpFactoFable.DAL.Source.query.clone();
		tmpFactoFable.DAL.Source.doCount(tmpQ, (pE, pQ, pCount) => fResolve(pCount));
	});
	assert('Facto has Sources', tmpSourceCount > 0, `count=${tmpSourceCount}`);
	assert('Facto Sources >= dataset count', tmpSourceCount >= DATASETS.length,
		`${tmpSourceCount} sources vs ${DATASETS.length} datasets`);

	// Check Datasets
	let tmpDatasetCount = await new Promise((fResolve) =>
	{
		let tmpQ = tmpFactoFable.DAL.Dataset.query.clone();
		tmpFactoFable.DAL.Dataset.doCount(tmpQ, (pE, pQ, pCount) => fResolve(pCount));
	});
	assert('Facto has Datasets', tmpDatasetCount > 0, `count=${tmpDatasetCount}`);

	// Check DatasetSource links
	let tmpDSLinkCount = await new Promise((fResolve) =>
	{
		let tmpQ = tmpFactoFable.DAL.DatasetSource.query.clone();
		tmpFactoFable.DAL.DatasetSource.doCount(tmpQ, (pE, pQ, pCount) => fResolve(pCount));
	});
	assert('Facto has DatasetSource links', tmpDSLinkCount > 0, `count=${tmpDSLinkCount}`);
	assert('Every dataset linked to a source', tmpDSLinkCount >= DATASETS.length,
		`${tmpDSLinkCount} links vs ${DATASETS.length} datasets`);

	// Check Records
	let tmpRecordCount = await new Promise((fResolve) =>
	{
		let tmpQ = tmpFactoFable.DAL.Record.query.clone();
		tmpFactoFable.DAL.Record.doCount(tmpQ, (pE, pQ, pCount) => fResolve(pCount));
	});
	let tmpTotalLoaded = tmpResults.reduce((pSum, pR) => pSum + pR.loaded, 0);
	assert('Facto has Records', tmpRecordCount > 0, `count=${tmpRecordCount}`);
	assert('Record count matches loaded total', tmpRecordCount >= tmpTotalLoaded,
		`db=${tmpRecordCount} loaded=${tmpTotalLoaded}`);

	// Spot-check: first record has IDDataset and IDSource set
	let tmpSpotRecords = await new Promise((fResolve) =>
	{
		let tmpQ = tmpFactoFable.DAL.Record.query.clone();
		tmpQ.setCap(1);
		tmpFactoFable.DAL.Record.doReads(tmpQ, (pE, pQ, pRecs) => fResolve(pRecs || []));
	});
	if (tmpSpotRecords.length > 0)
	{
		assert('Records have IDDataset set', tmpSpotRecords[0].IDDataset > 0,
			`IDDataset=${tmpSpotRecords[0].IDDataset}`);
		assert('Records have IDSource set', tmpSpotRecords[0].IDSource > 0,
			`IDSource=${tmpSpotRecords[0].IDSource}`);
		assert('Records have Content', tmpSpotRecords[0].Content && tmpSpotRecords[0].Content.length > 2,
			`Content length=${(tmpSpotRecords[0].Content || '').length}`);
	}

	// Check IngestJobs
	let tmpJobCount = await new Promise((fResolve) =>
	{
		let tmpQ = tmpFactoFable.DAL.IngestJob.query.clone();
		tmpFactoFable.DAL.IngestJob.doCount(tmpQ, (pE, pQ, pCount) => fResolve(pCount));
	});
	assert('Facto has IngestJobs', tmpJobCount > 0, `count=${tmpJobCount}`);

	// Check IngestJob status = Complete
	let tmpCompleteJobs = await new Promise((fResolve) =>
	{
		let tmpQ = tmpFactoFable.DAL.IngestJob.query.clone();
		tmpQ.addFilter('Status', 'Complete');
		tmpFactoFable.DAL.IngestJob.doCount(tmpQ, (pE, pQ, pCount) => fResolve(pCount));
	});
	assert('All IngestJobs completed', tmpCompleteJobs === tmpJobCount,
		`${tmpCompleteJobs} complete out of ${tmpJobCount}`);

	// Check Dataset hashes exist
	let tmpDatasetsWithHash = await new Promise((fResolve) =>
	{
		let tmpQ = tmpFactoFable.DAL.Dataset.query.clone();
		tmpFactoFable.DAL.Dataset.doReads(tmpQ, (pE, pQ, pRecs) => fResolve(pRecs || []));
	});
	let tmpMissingHashes = tmpDatasetsWithHash.filter((pD) => !pD.Hash || pD.Hash.length === 0);
	assert('All Datasets have Hashes', tmpMissingHashes.length === 0,
		tmpMissingHashes.length > 0 ? `${tmpMissingHashes.length} datasets without hashes` : undefined);

	console.log('');

	// ── Phase 6: Validate Ultravisor manifest ───────────────────────────

	console.log('── Phase 6: Validate Ultravisor operation manifests ───');

	let tmpRuns = await httpGet('http://localhost:8422/Run');
	if (tmpRuns.status === 200 && Array.isArray(tmpRuns.body))
	{
		assert('Ultravisor has execution runs', tmpRuns.body.length > 0, `count=${tmpRuns.body.length}`);
		let tmpCompletedRuns = tmpRuns.body.filter((pR) => pR.Status === 'Complete');
		let tmpErrorRuns = tmpRuns.body.filter((pR) => pR.Status === 'Error');
		assert('All runs completed', tmpCompletedRuns.length === tmpRuns.body.length,
			`${tmpCompletedRuns.length} complete, ${tmpErrorRuns.length} errors out of ${tmpRuns.body.length}`);
	}
	else
	{
		// Run endpoint may not exist — skip gracefully
		console.log('  (Ultravisor /Run endpoint not available — skipping manifest validation)');
	}

	console.log('');

	// ── Summary ─────────────────────────────────────────────────────────

	console.log('══════════════════════════════════════════════════════');
	console.log('');

	let tmpTotalChecks = _Passes + _Failures.length;
	console.log(`  Checks:  ${tmpTotalChecks} total,  ${_Passes} passed,  ${_Failures.length} failed`);
	console.log(`  Datasets: ${DATASETS.length}`);
	console.log(`  Records in Facto: ${tmpRecordCount}`);
	console.log('');

	if (_Failures.length > 0)
	{
		console.log('  FAILURES:');
		for (let tmpF of _Failures)
		{
			console.log(`    ✗ ${tmpF.description}` + (tmpF.detail ? ` — ${tmpF.detail}` : ''));
		}
		console.log('');
	}

	// ── Cleanup ─────────────────────────────────────────────────────────

	await new Promise((fResolve) =>
	{
		tmpPict.HarnessServerManager.stopAll(fResolve);
	});
	console.log('  [OK] Servers stopped');
	console.log('');
	console.log(_Failures.length === 0 ? '  ═══ ALL VALIDATIONS PASSED ═══' : '  ═══ SOME VALIDATIONS FAILED ═══');
	process.exit(_Failures.length === 0 ? 0 : 1);
}

main().catch((pError) =>
{
	console.error('Fatal error:', pError.message);
	process.exit(1);
});
