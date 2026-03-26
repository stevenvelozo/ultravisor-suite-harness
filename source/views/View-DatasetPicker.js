/**
 * View-DatasetPicker
 *
 * Lets the user choose which datasets to test.
 * Preset selection via keyboard — updates AppData.Harness.SelectedDatasets.
 * Renders to #Harness-Content.
 */
const libPictView = require('pict-view');

const _ViewConfiguration =
{
	ViewIdentifier: 'Harness-DatasetPicker',

	DefaultRenderable: 'DatasetPicker-Content',
	DefaultDestinationAddress: '#Harness-Content',
	DefaultTemplateRecordAddress: 'AppData.Harness',

	AutoRender: false,

	Templates:
	[
		{
			Hash: 'DatasetPicker-Template',
			Template: '{~D:Record.DatasetPickerText~}',
		},
	],

	Renderables:
	[
		{
			RenderableHash: 'DatasetPicker-Content',
			TemplateHash: 'DatasetPicker-Template',
			ContentDestinationAddress: '#Harness-Content',
			RenderMethod: 'replace',
		},
	],
};

class ViewDatasetPicker extends libPictView
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);
	}
}

module.exports = ViewDatasetPicker;
module.exports.default_configuration = _ViewConfiguration;
