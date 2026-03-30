/**
 * View-MainMenu
 *
 * Main menu for the Ultravisor Suite Harness.
 * Shows live server status and navigation options.
 * Renders to #Harness-Content.
 */
const libPictView = require('pict-view');

const _ViewConfiguration =
{
	ViewIdentifier: 'Harness-MainMenu',

	DefaultRenderable: 'MainMenu-Content',
	DefaultDestinationAddress: '#Harness-Content',
	DefaultTemplateRecordAddress: 'AppData.Harness',

	AutoRender: false,

	Templates:
	[
		{
			Hash: 'MainMenu-Template',
			Template: [
				'{bold}Ultravisor Suite Harness{/bold}  v{~D:Record.AppVersion~}',
				'',
				'{cyan-fg}── Servers ───────────────────────────────────────{/cyan-fg}',
				'',
				'  Facto Server       :8420   {~D:Record.Servers.Facto.statusLabel~}    http://localhost:8420/',
				'  Integration        :8421   {~D:Record.Servers.Integration.statusLabel~}    http://localhost:8421/mapping/  http://localhost:8421/docs/',
				'  Ultravisor         :8422   {~D:Record.Servers.Ultravisor.statusLabel~}    http://localhost:8422/',
				'',
				'  Library: {~D:Record.FactoLibraryPath~}',
				'  Data:    {~D:Record.DataDir~}',
				'',
				'{cyan-fg}── Options ───────────────────────────────────────{/cyan-fg}',
				'',
				'  {yellow-fg}[1]{/yellow-fg}  Clean & Execute',
				'       Stop servers, wipe data/, reinit with fresh DBs,',
				'       then run the full HTTP pipeline against selected datasets.',
				'',
				'  {yellow-fg}[2]{/yellow-fg}  Run Suite  (without cleaning)',
				'       Run the pipeline against the existing server state.',
				'       Useful for re-running after a partial failure.',
				'',
				'  {yellow-fg}[3]{/yellow-fg}  View Results',
				'       Show the pass/fail summary from the last run.',
				'',
				'  {yellow-fg}[4]{/yellow-fg}  Dataset Picker',
				'       Choose which facto-library datasets to test.',
				'',
				'  {yellow-fg}[5]{/yellow-fg}  Server Log',
				'       View captured console/server output.  (also: {yellow-fg}l{/yellow-fg} from any view)',
				'',
				'{cyan-fg}─────────────────────────────────────────────────{/cyan-fg}',
				'',
				'  {yellow-fg}[q]{/yellow-fg}  Quit',
				'',
				'Suite status: {~D:Record.SuiteStatus~}',
			].join('\n'),
		},
	],

	Renderables:
	[
		{
			RenderableHash: 'MainMenu-Content',
			TemplateHash: 'MainMenu-Template',
			ContentDestinationAddress: '#Harness-Content',
			RenderMethod: 'replace',
		},
	],
};

class ViewMainMenu extends libPictView
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);
	}

	/**
	 * Before rendering, compute the statusLabel blessed-tagged strings from
	 * the raw status values so the template gets pre-coloured text.
	 */
	onBeforeRender(pRenderable)
	{
		let tmpServers = this.pict.AppData.Harness && this.pict.AppData.Harness.Servers;

		if (tmpServers)
		{
			tmpServers.Facto.statusLabel       = _statusLabel(tmpServers.Facto.status);
			tmpServers.Integration.statusLabel  = _statusLabel(tmpServers.Integration.status);
			tmpServers.Ultravisor.statusLabel   = _statusLabel(tmpServers.Ultravisor.status);
		}
	}
}

/**
 * Convert a raw status string into a coloured blessed-tagged label.
 */
function _statusLabel(pStatus)
{
	switch (pStatus)
	{
		case 'running':
			return '{green-fg}[RUNNING]{/green-fg}';
		case 'starting':
			return '{yellow-fg}[STARTING]{/yellow-fg}';
		case 'stopping':
			return '{yellow-fg}[STOPPING]{/yellow-fg}';
		case 'error':
			return '{red-fg}[ERROR]{/red-fg}';
		default:
			return '{gray-fg}[PENDING]{/gray-fg}';
	}
}

module.exports = ViewMainMenu;
module.exports.default_configuration = _ViewConfiguration;
