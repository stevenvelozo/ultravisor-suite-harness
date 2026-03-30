#!/usr/bin/env node
/**
 * Ultravisor Suite Harness
 *
 * End-to-end test harness for the facto pipeline: parse -> map -> transform -> load.
 * Uses real datasets from the facto-library and a local SQLite database.
 *
 * Run:     node harness.js
 * Quit:    q or Ctrl-C
 * Nav:     1-5 on main menu, m = main menu from any view
 */

// ── Console capture ─────────────────────────────────────────────────────────
// Intercept stdout AND stderr before anything writes.  Captured lines are
// stored in a ring buffer that the blessed log view can read.  Once blessed
// owns the terminal (_ScreenActive === true) we swallow raw writes so they
// never corrupt the screen.

const _ConsoleBuffer = [];
const MAX_CONSOLE_LINES = 2000;
let _ScreenActive = false;

// Expose via global so Harness-Application (and any other module) can read
// the buffer and flip the screen-active flag without a require() cycle.
global._HarnessConsoleBuffer = _ConsoleBuffer;
global._HarnessSetScreenActive = function (pActive) { _ScreenActive = pActive; };

function _captureWrite(pOrigWrite, pChunk, pEncoding, pCallback)
{
	let tmpStr = (typeof pChunk === 'string') ? pChunk : pChunk.toString();

	// Only buffer plain text lines (not blessed escape sequences).
	// Blessed renders by writing escape codes to stdout — we must
	// never swallow those or the screen goes blank.
	if (tmpStr.indexOf('\x1b[') === -1)
	{
		let tmpLines = tmpStr.split('\n');
		for (let i = 0; i < tmpLines.length; i++)
		{
			let tmpLine = tmpLines[i];
			if (tmpLine.length === 0 && i === tmpLines.length - 1)
			{
				continue; // trailing newline split artifact
			}
			_ConsoleBuffer.push(tmpLine);
		}
		while (_ConsoleBuffer.length > MAX_CONSOLE_LINES)
		{
			_ConsoleBuffer.shift();
		}
	}

	// Always pass through to the real write — blessed needs stdout
	// to paint the terminal.  Fable log noise is suppressed by using
	// empty LogStreams on child server instances.
	return pOrigWrite(pChunk, pEncoding, pCallback);
}

const _origStdoutWrite = process.stdout.write.bind(process.stdout);
const _origStderrWrite = process.stderr.write.bind(process.stderr);

process.stdout.write = function (pChunk, pEncoding, pCallback)
{
	return _captureWrite(_origStdoutWrite, pChunk, pEncoding, pCallback);
};

process.stderr.write = function (pChunk, pEncoding, pCallback)
{
	// Still suppress blessed's Setulc noise entirely (not even buffered)
	if (typeof pChunk === 'string' && pChunk.indexOf('Setulc') !== -1)
	{
		return true;
	}
	return _captureWrite(_origStderrWrite, pChunk, pEncoding, pCallback);
};

// ── Port cleanup ─────────────────────────────────────────────────────────────
// Kill any stale processes on our ports from a previous run that did not
// shut down cleanly (e.g. Ctrl-C without a SIGINT handler).

const { execSync } = require('child_process');
const HARNESS_PORTS = [8420, 8421, 8422];

function _killStalePortProcesses()
{
	for (let i = 0; i < HARNESS_PORTS.length; i++)
	{
		let tmpPort = HARNESS_PORTS[i];
		try
		{
			// lsof to find PID, filter out our own PID
			let tmpPids = execSync(`lsof -ti:${tmpPort} 2>/dev/null`, { encoding: 'utf8' })
				.trim().split('\n')
				.filter((pPid) => pPid.length > 0 && parseInt(pPid) !== process.pid);

			for (let j = 0; j < tmpPids.length; j++)
			{
				try
				{
					process.kill(parseInt(tmpPids[j]), 'SIGTERM');
					_origStdoutWrite(`[harness] Killed stale process ${tmpPids[j]} on port ${tmpPort}\n`);
				}
				catch (pKillErr)
				{
					// Process may have already exited
				}
			}
		}
		catch (pLsofErr)
		{
			// No process on this port — nothing to do
		}
	}
}

_killStalePortProcesses();

// ── CLI argument parsing ────────────────────────────────────────────────────

const HEADLESS = process.argv.includes('--headless');
const DATASETS_ARG = process.argv.find((pArg) => pArg.startsWith('--datasets='));

// ── Bootstrap ───────────────────────────────────────────────────────────────

const libPict = require('pict');
const libPictApplication = require('pict-application');
const libHarnessApplication = require('./source/Harness-Application.js');

if (HEADLESS)
{
	// ── Headless mode ───────────────────────────────────────────────────────
	// Skip the blessed TUI entirely and run the Clean & Execute pipeline
	// directly.  Usage:
	//   node harness.js --headless
	//   node harness.js --headless --datasets=datahub-country-codes,datahub-currency-codes

	const libPath = require('path');
	const ServiceServerManager = require('./source/services/Service-ServerManager.js');
	const ServiceDataManager = require('./source/services/Service-DataManager.js');
	const ServiceTestOrchestrator = require('./source/services/Service-TestOrchestrator.js');

	const DATA_DIR = libPath.resolve(__dirname, 'data');
	const FACTO_LIB = libPath.resolve(__dirname, '..', 'dist', 'facto-library');
	const DEFAULT_DATASETS = ['datahub-country-codes', 'datahub-currency-codes'];
	const DATASETS = DATASETS_ARG
		? DATASETS_ARG.split('=')[1].split(',').map((pD) => pD.trim())
		: DEFAULT_DATASETS;

	let tmpPict = new libPict({ Product: 'UltravisorSuiteHarness', LogNoisiness: 0 });
	tmpPict.addAndInstantiateServiceTypeIfNotExists('HarnessServerManager', ServiceServerManager);
	tmpPict.addAndInstantiateServiceTypeIfNotExists('HarnessDataManager', ServiceDataManager);
	tmpPict.addAndInstantiateServiceTypeIfNotExists('HarnessTestOrchestrator', ServiceTestOrchestrator);

	console.log('=== Ultravisor Suite Harness (headless) ===');
	console.log(`Datasets: ${DATASETS.join(', ')}`);
	console.log('');

	tmpPict.HarnessDataManager.cleanDataDir(DATA_DIR,
		(pCleanError) =>
		{
			if (pCleanError) { console.error('Clean failed:', pCleanError.message); process.exit(1); }
			console.log('[OK] Data directory cleaned');

			tmpPict.HarnessServerManager.startAll(DATA_DIR,
				(pServerError) =>
				{
					if (pServerError) { console.error('Server start failed:', pServerError.message); process.exit(1); }
					console.log('[OK] Servers running (Facto :8420  Ultravisor :8422)');

					tmpPict.HarnessDataManager.initDatabase(DATA_DIR,
						(pInitError) =>
						{
							if (pInitError) { console.error('DB init failed:', pInitError.message); process.exit(1); }
							console.log('[OK] Harness DB initialized');
							console.log('');

							tmpPict.HarnessTestOrchestrator.runSuite(DATASETS, FACTO_LIB, DATA_DIR,
								(pProgress) => { console.log(' ', pProgress); },
								(pSuiteError, pResults) =>
								{
									console.log('');
									console.log('=== Results ===');
									if (pResults)
									{
										pResults.forEach(
											(pResult) =>
											{
												console.log('  ' + pResult.dataset.padEnd(30) + ' ' +
													pResult.status.toUpperCase().padEnd(6) +
													' parsed=' + String(pResult.parsed).padStart(4) +
													' loaded=' + String(pResult.loaded).padStart(4) +
													' verified=' + String(pResult.verified).padStart(4) +
													(pResult.error ? '  ERROR: ' + pResult.error : ''));
											});
										let tmpPassed = pResults.filter((pR) => pR.status === 'pass').length;
										let tmpFailed = pResults.filter((pR) => pR.status !== 'pass').length;
										console.log('');
										console.log('Summary: ' + tmpPassed + ' passed, ' + tmpFailed + ' failed');
									}

									console.log('');
									tmpPict.HarnessServerManager.stopAll(
										() =>
										{
											console.log('[OK] Servers stopped');
											let tmpAllPass = pResults && pResults.every((pR) => pR.status === 'pass');
											console.log(tmpAllPass ? '=== ALL TESTS PASSED ===' : '=== SOME TESTS FAILED ===');
											process.exit(tmpAllPass ? 0 : 1);
										});
								});
						});
				});
		});
}
else
{

let _Pict = new libPict(
	{
		Product: 'UltravisorSuiteHarness',
		LogNoisiness: 0,
		// No console log stream — blessed owns stdout.  Server child
		// instances also use LogStreams: [].  The log view reads from
		// the console buffer which captures any stray writes.
		LogStreams: [],
	});

let _App = _Pict.addApplication('Harness',
	{
		Name: 'Harness',
		MainViewportViewIdentifier: 'Harness-MainMenu',
		AutoRenderMainViewportViewAfterInitialize: false,
		AutoSolveAfterInitialize: false,
	}, libHarnessApplication);

// ── Graceful shutdown ────────────────────────────────────────────────────────
// Catch SIGINT (Ctrl-C) so we stop servers and free ports before exiting.
// Without this, a killed harness leaves ports bound and the next run hangs.

let _ShuttingDown = false;
function _gracefulShutdown()
{
	if (_ShuttingDown)
	{
		return;
	}
	_ShuttingDown = true;

	// Restore stdout so shutdown messages are visible
	_ScreenActive = false;

	// Tear down blessed screen if it exists
	if (_App && _App.terminalUI && typeof _App.terminalUI.destroyScreen === 'function')
	{
		try { _App.terminalUI.destroyScreen(); }
		catch (pErr) { /* non-fatal */ }
	}

	_origStdoutWrite('\n[harness] Shutting down...\n');

	if (_Pict && _Pict.HarnessServerManager)
	{
		_Pict.HarnessServerManager.stopAll(
			() =>
			{
				_origStdoutWrite('[harness] Servers stopped. Goodbye.\n');
				process.exit(0);
			});

		// Force exit if stopAll hangs for more than 3 seconds
		setTimeout(() => { process.exit(0); }, 3000);
	}
	else
	{
		process.exit(0);
	}
}

process.on('SIGINT', _gracefulShutdown);
process.on('SIGTERM', _gracefulShutdown);

// ── Startup timeout ──────────────────────────────────────────────────────────
// If initialization takes more than 30 seconds, something is wrong.

let _StartupTimer = setTimeout(
	() =>
	{
		_ScreenActive = false;
		_origStderrWrite('[harness] ERROR: Startup timed out after 30 seconds.\n');
		_gracefulShutdown();
	}, 30000);

_App.initializeAsync(
	(pError) =>
	{
		clearTimeout(_StartupTimer);

		if (pError)
		{
			// Restore stdout so the error is visible even if blessed tried to start
			_ScreenActive = false;
			console.error('Harness initialization failed:', pError);
			process.exit(1);
		}
	});

} // end of TUI (non-headless) branch
