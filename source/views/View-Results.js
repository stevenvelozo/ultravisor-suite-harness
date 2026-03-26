/**
 * View-Results
 *
 * Displays the pass/fail summary table from the last suite run.
 * Renders to #Harness-Content.
 */
const libPictView = require('pict-view');

const _ViewConfiguration =
{
	ViewIdentifier: 'Harness-Results',

	DefaultRenderable: 'Results-Content',
	DefaultDestinationAddress: '#Harness-Content',
	DefaultTemplateRecordAddress: 'AppData.Harness',

	AutoRender: false,

	Templates:
	[
		{
			Hash: 'Results-Template',
			Template: '{~D:Record.ResultsText~}',
		},
	],

	Renderables:
	[
		{
			RenderableHash: 'Results-Content',
			TemplateHash: 'Results-Template',
			ContentDestinationAddress: '#Harness-Content',
			RenderMethod: 'replace',
		},
	],
};

class ViewResults extends libPictView
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);
	}
}

module.exports = ViewResults;
module.exports.default_configuration = _ViewConfiguration;
