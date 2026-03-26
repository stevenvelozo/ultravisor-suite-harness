/**
 * Harness Application
 *
 * Main blessed-ui pict application class for the Ultravisor Suite Harness.
 * Creates the terminal layout and manages navigation between views.
 *
 * Startup sequence:
 *   1. Initialize blessed UI layout
 *   2. Start Facto server  (port 8420)
 *   3. Start Target server (port 8421)
 *   4. Render main menu with live server status
 */
const libPictApplication = require('pict-application');
const libPictTerminalUI = require('pict-terminalui');
const libBlessed = require('blessed');
const libPath = require('path');

const libViewMainMenu = require('./views/View-MainMenu.js');
const libViewSuiteRunner = require('./views/View-SuiteRunner.js');
const libViewResults = require('./views/View-Results.js');
const libViewDatasetPicker = require('./views/View-DatasetPicker.js');
const libViewLog = require('./views/View-Log.js');

const libServiceDataManager = require('./services/Service-DataManager.js');
const libServiceTestOrchestrator = require('./services/Service-TestOrchestrator.js');
const libServiceServerManager = require('./services/Service-ServerManager.js');

// Path to the facto-library relative to this file
const FACTO_LIBRARY_PATH = libPath.resolve(__dirname, '..', '..', 'dist', 'facto-library');
const DATA_DIR_PATH = libPath.resolve(__dirname, '..', 'data');

// Small preset: fast CSVs suitable for a quick smoke test
const PRESET_SMALL = ['datahub-country-codes', 'datahub-currency-codes', 'datahub-language-codes'];

// Medium preset: adds structured reference datasets + bookstore multi-entity
const PRESET_MEDIUM = [
	'datahub-country-codes', 'datahub-currency-codes', 'datahub-language-codes',
	'iana-tlds', 'ral-colors', 'bls-sic-titles', 'bls-soc-2018',
	'bookstore',
];

// Large preset: adds larger datasets exercising scale
const PRESET_LARGE = [
	'datahub-country-codes', 'datahub-currency-codes', 'datahub-language-codes',
	'iana-tlds', 'ral-colors', 'bls-sic-titles', 'bls-soc-2018',
	'iso-10383-mic-codes', 'ipeds', 'nflverse', 'ieee-oui',
	'ourairports', 'tiger-relationship-files', 'project-gutenberg-catalog',
	'bookstore',
];

class HarnessApplication extends libPictApplication
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);

		this.terminalUI = null;
		this.currentRoute = 'MainMenu';
		this._screen = null;
		this._headerWidget = null;
		this._statusWidget = null;
		this._contentBox = null;

		// Register services
		this.pict.addAndInstantiateServiceTypeIfNotExists('HarnessDataManager', libServiceDataManager);
		this.pict.addAndInstantiateServiceTypeIfNotExists('HarnessTestOrchestrator', libServiceTestOrchestrator);
		this.pict.addAndInstantiateServiceTypeIfNotExists('HarnessServerManager', libServiceServerManager);

		// Add views
		this.pict.addView('Harness-MainMenu', libViewMainMenu.default_configuration, libViewMainMenu);
		this.pict.addView('Harness-SuiteRunner', libViewSuiteRunner.default_configuration, libViewSuiteRunner);
		this.pict.addView('Harness-Results', libViewResults.default_configuration, libViewResults);
		this.pict.addView('Harness-DatasetPicker', libViewDatasetPicker.default_configuration, libViewDatasetPicker);
		this.pict.addView('Harness-Log', libViewLog.default_configuration, libViewLog);
	}

	onAfterInitializeAsync(fCallback)
	{
		// Initialize shared application state
		this.pict.AppData.Harness =
		{
			AppName: 'Ultravisor Suite Harness',
			AppVersion: '0.0.1',
			CurrentRoute: 'MainMenu',
			StatusMessage: 'Starting servers...',
			DataDir: DATA_DIR_PATH,
			FactoLibraryPath: FACTO_LIBRARY_PATH,
			SelectedDatasets: PRESET_LARGE.slice(),
			AvailablePresets: { small: PRESET_SMALL, medium: PRESET_MEDIUM, large: PRESET_LARGE },
			SuiteLog: 'No run started yet.\n\nPress 1 from the main menu to clean and execute.',
			SuiteStatus: 'idle',
			ResultsText: 'No results yet. Run the suite first.',
			DatasetPickerText: '',
			ConsoleLog: '',
			Servers:
			{
				Facto:      { port: 8420, status: 'starting' },
				Ultravisor: { port: 8422, status: 'starting' },
			},
		};

		// Build the blessed UI first so we can show startup progress
		this.terminalUI = new libPictTerminalUI(this.pict, { Title: 'Ultravisor Suite Harness' });

		this._screen = this.terminalUI.createScreen();

		// Tell the stdout/stderr intercept that blessed now owns the terminal.
		if (typeof global._HarnessSetScreenActive === 'function')
		{
			global._HarnessSetScreenActive(true);
		}

		this._createBlessedLayout(this._screen);
		this._bindNavigation(this._screen);

		// Initial render while servers are starting
		this._setHeader('Main Menu');
		this._setStatus('Starting servers: Facto (8420), Target (8421), Ultravisor (8422)...');
		this.pict.views['Harness-MainMenu'].render();
		this._screen.render();

		// Ensure data directory exists before starting servers
		const libFs = require('fs');
		try
		{
			libFs.mkdirSync(DATA_DIR_PATH, { recursive: true });
		}
		catch (pMkdirError)
		{
			// Non-fatal — servers will fail gracefully if the dir is missing
		}

		// Start both servers, then refresh the menu
		this.pict.HarnessServerManager.startAll(DATA_DIR_PATH,
			(pServerError) =>
			{
				if (pServerError)
				{
					this.pict.AppData.Harness.Servers.Facto.status      = 'error';
	
					this.pict.AppData.Harness.Servers.Ultravisor.status = 'error';
					this._setStatus(`Server start failed: ${pServerError.message}  |  q=Quit`);
				}
				else
				{
					this.pict.AppData.Harness.Servers.Facto.status      = 'running';
	
					this.pict.AppData.Harness.Servers.Ultravisor.status = 'running';
					this._setStatus('Ready  |  1=Clean & Execute  2=Run  3=Results  4=Datasets  5=Log  q=Quit');
				}

				// Re-render the main menu to show updated server status
				this.pict.views['Harness-MainMenu'].render();
				this._screen.render();

				return super.onAfterInitializeAsync(fCallback);
			});
	}

	// ─────────────────────────────────────────────
	//  Layout
	// ─────────────────────────────────────────────

	_createBlessedLayout(pScreen)
	{
		// Header bar
		this._headerWidget = libBlessed.box(
			{
				parent: pScreen,
				top: 0,
				left: 0,
				width: '100%',
				height: 3,
				tags: true,
				style:
				{
					fg: 'white',
					bg: 'blue',
					bold: true,
				},
			});
		this.terminalUI.registerWidget('#Harness-Header', this._headerWidget);

		// Main content area
		this._contentBox = libBlessed.box(
			{
				parent: pScreen,
				top: 3,
				left: 0,
				width: '100%',
				bottom: 1,
				tags: true,
				scrollable: true,
				alwaysScroll: true,
				mouse: true,
				keys: true,
				vi: true,
				scrollbar:
				{
					style: { bg: 'cyan' },
				},
				border:
				{
					type: 'line',
				},
				style:
				{
					border: { fg: 'cyan' },
				},
				label: ' Main Menu ',
				padding:
				{
					left: 1,
					right: 1,
				},
			});
		this.terminalUI.registerWidget('#Harness-Content', this._contentBox);

		// Status bar
		this._statusWidget = libBlessed.box(
			{
				parent: pScreen,
				bottom: 0,
				left: 0,
				width: '100%',
				height: 1,
				tags: true,
				style:
				{
					fg: 'white',
					bg: 'gray',
				},
			});
		this.terminalUI.registerWidget('#Harness-StatusBar', this._statusWidget);
	}

	// ─────────────────────────────────────────────
	//  Navigation
	// ─────────────────────────────────────────────

	_bindNavigation(pScreen)
	{
		pScreen.key(['q', 'C-c'], () => { this.terminalUI.destroyScreen(); });
		pScreen.key(['m'], () => { this.navigateTo('MainMenu'); });

		// Main menu number keys
		pScreen.key(['1'], () => { this._handleMainMenuKey(1); });
		pScreen.key(['2'], () => { this._handleMainMenuKey(2); });
		pScreen.key(['3'], () => { this._handleMainMenuKey(3); });
		pScreen.key(['4'], () => { this._handleMainMenuKey(4); });
		pScreen.key(['5'], () => { this._handleMainMenuKey(5); });

		// Global shortcut: 'l' for log from any view
		pScreen.key(['l'], () => { this._showLogView(); });

		// Dataset picker preset keys
		pScreen.key(['s'], () => { this._handleDatasetPickerKey('s'); });
		pScreen.key(['M'], () => { this._handleDatasetPickerKey('M'); });
	}

	_handleMainMenuKey(pOption)
	{
		if (this.currentRoute !== 'MainMenu')
		{
			return;
		}
		switch (pOption)
		{
			case 1:
				this._cleanAndExecute();
				break;
			case 2:
				this._runSuite();
				break;
			case 3:
				this.navigateTo('Results');
				break;
			case 4:
				this._openDatasetPicker();
				break;
			case 5:
				this._showLogView();
				break;
		}
	}

	_handleDatasetPickerKey(pKey)
	{
		if (this.currentRoute !== 'DatasetPicker')
		{
			return;
		}

		let tmpPresets = this.pict.AppData.Harness.AvailablePresets;

		if (pKey === 's')
		{
			this.pict.AppData.Harness.SelectedDatasets = tmpPresets.small.slice();
			this._updateDatasetPickerText();
			this._setStatus(`Preset: Small (${tmpPresets.small.length} datasets)  |  m=MainMenu`);
			this.pict.views['Harness-DatasetPicker'].render();
			this._screen.render();
		}
		else if (pKey === 'M')
		{
			this.pict.AppData.Harness.SelectedDatasets = tmpPresets.medium.slice();
			this._updateDatasetPickerText();
			this._setStatus(`Preset: Medium (${tmpPresets.medium.length} datasets)  |  m=MainMenu`);
			this.pict.views['Harness-DatasetPicker'].render();
			this._screen.render();
		}
		else if (pKey === 'L')
		{
			this.pict.AppData.Harness.SelectedDatasets = tmpPresets.large.slice();
			this._updateDatasetPickerText();
			this._setStatus(`Preset: Large (${tmpPresets.large.length} datasets)  |  m=MainMenu`);
			this.pict.views['Harness-DatasetPicker'].render();
			this._screen.render();
		}
	}

	navigateTo(pRoute)
	{
		let tmpViewName = `Harness-${pRoute}`;
		if (!(tmpViewName in this.pict.views))
		{
			return;
		}

		this.currentRoute = pRoute;
		this.pict.AppData.Harness.CurrentRoute = pRoute;

		if (this._contentBox)
		{
			this._contentBox.setLabel(` ${pRoute} `);
			this._contentBox.scrollTo(0);
		}

		this.pict.views[tmpViewName].render();
		this._screen.render();
	}

	// ─────────────────────────────────────────────
	//  Pipeline operations
	// ─────────────────────────────────────────────

	_cleanAndExecute()
	{
		this._setHeader('Suite Runner');
		this._setStatus('Stopping servers...');
		this.pict.AppData.Harness.SuiteStatus = 'cleaning';
		this.pict.AppData.Harness.SuiteLog    = '[CLEAN] Stopping servers...\n';
		this.navigateTo('SuiteRunner');

		// Stop servers → wipe data dir → reinitialize DB → restart servers → run suite
		this.pict.AppData.Harness.Servers.Facto.status      = 'stopping';

		this.pict.AppData.Harness.Servers.Ultravisor.status = 'stopping';

		this.pict.HarnessServerManager.stopAll(
			() =>
			{
				this._appendLog('[CLEAN] Servers stopped.\n[CLEAN] Wiping data directory...\n');
				this._setStatus('Cleaning data directory...');

				this.pict.HarnessDataManager.cleanDataDir(DATA_DIR_PATH,
					(pCleanError) =>
					{
						if (pCleanError)
						{
							this._appendLog(`[ERROR] Clean failed: ${pCleanError.message}\n`);
							this._setStatus(`Clean failed: ${pCleanError.message}`);
							return;
						}

						this._appendLog('[CLEAN] Data directory wiped.\n[INIT] Starting fresh servers...\n');
						this._setStatus('Starting fresh servers...');

						this.pict.AppData.Harness.Servers.Facto.status      = 'starting';

						this.pict.AppData.Harness.Servers.Ultravisor.status = 'starting';

						this.pict.HarnessServerManager.startAll(DATA_DIR_PATH,
							(pServerError) =>
							{
								if (pServerError)
								{
									this.pict.AppData.Harness.Servers.Facto.status      = 'error';
					
									this.pict.AppData.Harness.Servers.Ultravisor.status = 'error';
									this._appendLog(`[ERROR] Server restart failed: ${pServerError.message}\n`);
									this._setStatus(`Server restart failed: ${pServerError.message}`);
									return;
								}

								this.pict.AppData.Harness.Servers.Facto.status      = 'running';
				
								this.pict.AppData.Harness.Servers.Ultravisor.status = 'running';
								this._appendLog('[INIT] Servers running.\n[INIT] Initializing harness DB...\n');
								this._setStatus('Initializing harness database...');

								this.pict.HarnessDataManager.initDatabase(DATA_DIR_PATH,
									(pInitError) =>
									{
										if (pInitError)
										{
											this._appendLog(`[ERROR] Database init failed: ${pInitError.message}\n`);
											this._setStatus(`Init failed: ${pInitError.message}`);
											return;
										}

										this._appendLog('[INIT] Database ready.\n\n');
										this._runSuiteInternal();
									});
							});
					});
			});
	}

	_runSuite()
	{
		this._setHeader('Suite Runner');
		this.pict.AppData.Harness.SuiteLog = '';
		this.navigateTo('SuiteRunner');
		this._runSuiteInternal();
	}

	_runSuiteInternal()
	{
		let tmpDatasets   = this.pict.AppData.Harness.SelectedDatasets;
		let tmpFactoPath  = this.pict.AppData.Harness.FactoLibraryPath;

		this.pict.AppData.Harness.SuiteStatus = 'running';
		this._setStatus(`Running suite: ${tmpDatasets.length} datasets...`);
		this._appendLog(`[SUITE] Starting with ${tmpDatasets.length} dataset(s):\n`);
		tmpDatasets.forEach((pName) => { this._appendLog(`        - ${pName}\n`); });
		this._appendLog('\n');

		this.pict.HarnessTestOrchestrator.runSuite(
			tmpDatasets,
			tmpFactoPath,
			DATA_DIR_PATH,
			(pProgressMessage) =>
			{
				this._appendLog(`${pProgressMessage}\n`);
				this._setStatus(pProgressMessage);
			},
			(pError, pResults) =>
			{
				this.pict.AppData.Harness.SuiteStatus  = pError ? 'failed' : 'complete';
				this.pict.AppData.Harness.SuiteResults = pResults || [];
				this._buildResultsText(pResults || []);

				if (pError)
				{
					this._appendLog(`\n[ERROR] Suite failed: ${pError.message}\n`);
					this._setStatus(`Suite failed: ${pError.message}  |  m=MainMenu`);
				}
				else
				{
					let tmpPassed = (pResults || []).filter((r) => r.status === 'pass').length;
					this._appendLog(`\n[DONE] Suite complete: ${tmpPassed}/${(pResults || []).length} passed.\n`);
					this._appendLog('       Press 3 to view detailed results.\n');
					this._setStatus(`Complete: ${tmpPassed}/${(pResults || []).length} passed  |  3=Results  m=MainMenu`);
				}
			});
	}

	_openDatasetPicker()
	{
		this._setHeader('Dataset Picker');
		this._setStatus('s=Small preset  M=Medium preset  m=MainMenu');
		this._updateDatasetPickerText();
		this.navigateTo('DatasetPicker');
	}

	_showLogView()
	{
		this._setHeader('Server Log');
		this._setStatus('Captured console output  |  l=Refresh  m=MainMenu  q=Quit');
		this.navigateTo('Log');
	}

	// ─────────────────────────────────────────────
	//  AppData helpers
	// ─────────────────────────────────────────────

	_appendLog(pMessage)
	{
		this.pict.AppData.Harness.SuiteLog += pMessage;
		this.pict.views['Harness-SuiteRunner'] && this.pict.views['Harness-SuiteRunner'].render();
		if (this._contentBox)
		{
			this._contentBox.setScrollPerc(100);
		}
		this._screen && this._screen.render();
	}

	_setHeader(pRouteName)
	{
		if (this._headerWidget)
		{
			let tmpVersion = this.pict.AppData.Harness.AppVersion;
			this._headerWidget.setContent(
				` {bold}Ultravisor Suite Harness{/bold}  v${tmpVersion}  |  ${pRouteName}`
			);
		}
	}

	_setStatus(pMessage)
	{
		this.pict.AppData.Harness.StatusMessage = pMessage;
		if (this._statusWidget)
		{
			this._statusWidget.setContent(`  ${pMessage}`);
		}
	}

	_buildResultsText(pResults)
	{
		if (!pResults || pResults.length === 0)
		{
			this.pict.AppData.Harness.ResultsText = 'No results available.';
			return;
		}

		let tmpLines = [];
		tmpLines.push('{bold}Dataset Results{/bold}');
		tmpLines.push('');
		tmpLines.push('{cyan-fg}Dataset Name                   | Status | Parsed  | Loaded  | Verified{/cyan-fg}');
		tmpLines.push('─'.repeat(72));

		pResults.forEach(
			(pResult) =>
			{
				let tmpName     = (pResult.dataset || '').padEnd(30).substring(0, 30);
				let tmpStatus   = (pResult.status || '???').toUpperCase().padEnd(6);
				let tmpParsed   = String(pResult.parsed   || 0).padStart(7);
				let tmpLoaded   = String(pResult.loaded   || 0).padStart(7);
				let tmpVerified = String(pResult.verified || 0).padStart(8);

				let tmpStatusColored = pResult.status === 'pass'
					? `{green-fg}${tmpStatus}{/green-fg}`
					: (pResult.status === 'skip'
						? `{yellow-fg}${tmpStatus}{/yellow-fg}`
						: `{red-fg}${tmpStatus}{/red-fg}`);

				tmpLines.push(`${tmpName} | ${tmpStatusColored} | ${tmpParsed} | ${tmpLoaded} | ${tmpVerified}`);

				if (pResult.error)
				{
					tmpLines.push(`  {red-fg}^ ${pResult.error}{/red-fg}`);
				}
			});

		tmpLines.push('');
		let tmpPassed  = pResults.filter((r) => r.status === 'pass').length;
		let tmpFailed  = pResults.filter((r) => r.status === 'fail').length;
		let tmpSkipped = pResults.filter((r) => r.status === 'skip').length;
		tmpLines.push(`{bold}Summary:{/bold} ${tmpPassed} passed  ${tmpFailed} failed  ${tmpSkipped} skipped`);

		this.pict.AppData.Harness.ResultsText = tmpLines.join('\n');
	}

	_updateDatasetPickerText()
	{
		let tmpSelected = this.pict.AppData.Harness.SelectedDatasets;
		let tmpPresets  = this.pict.AppData.Harness.AvailablePresets;
		let tmpLines    = [];

		tmpLines.push('{bold}Dataset Picker{/bold}');
		tmpLines.push('');
		tmpLines.push('Select a preset to use for the next suite run:');
		tmpLines.push('');
		tmpLines.push('  {yellow-fg}[s]{/yellow-fg}  Small preset  — fast smoke test (3 datasets)');
		tmpPresets.small.forEach((pName) => { tmpLines.push(`         ${pName}`); });
		tmpLines.push('');
		tmpLines.push('  {yellow-fg}[M]{/yellow-fg}  Medium preset — broader coverage (5 datasets)');
		tmpPresets.medium.forEach((pName) => { tmpLines.push(`         ${pName}`); });
		tmpLines.push('');
		tmpLines.push('─'.repeat(60));
		tmpLines.push('{bold}Currently selected:{/bold}');
		tmpSelected.forEach((pName) => { tmpLines.push(`  {green-fg}✓{/green-fg} ${pName}`); });
		tmpLines.push('');
		tmpLines.push('Press {yellow-fg}m{/yellow-fg} to return to the main menu.');

		this.pict.AppData.Harness.DatasetPickerText = tmpLines.join('\n');
	}

	/**
	 * Called from View-MainMenu template.  Returns empty string.
	 */
	renderLayoutWidgets()
	{
		return '';
	}
}

module.exports = HarnessApplication;
