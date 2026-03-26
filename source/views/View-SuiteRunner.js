/**
 * View-SuiteRunner
 *
 * Real-time progress view shown while the test suite runs.
 * Renders to #Harness-Content and updates as each dataset is processed.
 */
const libPictView = require('pict-view');

const _ViewConfiguration =
{
	ViewIdentifier: 'Harness-SuiteRunner',

	DefaultRenderable: 'SuiteRunner-Content',
	DefaultDestinationAddress: '#Harness-Content',
	DefaultTemplateRecordAddress: 'AppData.Harness',

	AutoRender: false,

	Templates:
	[
		{
			Hash: 'SuiteRunner-Template',
			Template: '{~D:Record.SuiteLog~}',
		},
	],

	Renderables:
	[
		{
			RenderableHash: 'SuiteRunner-Content',
			TemplateHash: 'SuiteRunner-Template',
			ContentDestinationAddress: '#Harness-Content',
			RenderMethod: 'replace',
		},
	],
};

class ViewSuiteRunner extends libPictView
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);
	}
}

module.exports = ViewSuiteRunner;
module.exports.default_configuration = _ViewConfiguration;
