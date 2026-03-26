/**
 * View-Log
 *
 * Displays captured console/log output inside the blessed UI instead of
 * letting it corrupt the terminal.  Reads from the global ring buffer
 * populated by the stdout/stderr intercept in harness.js.
 *
 * The view pulls from global._HarnessConsoleBuffer on every render so it
 * always shows the most recent output.
 */
const libPictView = require('pict-view');

const _ViewConfiguration =
{
	ViewIdentifier: 'Harness-Log',

	DefaultRenderable: 'Log-Content',
	DefaultDestinationAddress: '#Harness-Content',
	DefaultTemplateRecordAddress: 'AppData.Harness',

	AutoRender: false,

	Templates:
	[
		{
			Hash: 'Log-Template',
			Template: '{~D:Record.ConsoleLog~}',
		},
	],

	Renderables:
	[
		{
			RenderableHash: 'Log-Content',
			TemplateHash: 'Log-Template',
			ContentDestinationAddress: '#Harness-Content',
			RenderMethod: 'replace',
		},
	],
};

class ViewLog extends libPictView
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);
	}

	/**
	 * Before rendering, snapshot the global console buffer into AppData so
	 * the template can read it.
	 */
	onBeforeRender()
	{
		let tmpBuffer = global._HarnessConsoleBuffer || [];

		if (tmpBuffer.length === 0)
		{
			this.pict.AppData.Harness.ConsoleLog = '{gray-fg}(no log output captured yet){/gray-fg}';
			return;
		}

		// Sanitize log lines so blessed tags inside raw output (JSON braces,
		// etc.) are not interpreted.  Replace { → [ and } → ] — this keeps
		// the output readable without confusing blessed's tag parser.
		let tmpLines = tmpBuffer.map(
			(pLine) =>
			{
				return pLine
					.replace(/\{/g, '[')
					.replace(/\}/g, ']');
			});

		let tmpText = '{bold}Server / Console Log{/bold}  (' + tmpBuffer.length + ' lines)\n'
			+ '{cyan-fg}' + '\u2500'.repeat(60) + '{/cyan-fg}\n'
			+ tmpLines.join('\n');

		this.pict.AppData.Harness.ConsoleLog = tmpText;
	}
}

module.exports = ViewLog;
module.exports.default_configuration = _ViewConfiguration;
