import json
from builtins import super

from django import forms


class GroutEditorWidget(forms.Widget):
  class Media:
    css = {'all': ('jsoneditor.css',)}
    js = (
#      'dist/grouteditor/main.js',
#      'dist/grouteditor/polyfills.js',
#      'dist/grouteditor/styles.js',
#      'dist/grouteditor/runtime.js',
#      'dist/grouteditor/vendor.js',
#      'dist/grouteditor/main.js',
    )
  template_name = 'grouteditor.html'

  def __init__(self, attrs=None, mode='code', options=None, width=None, height=None):
    default_options = {
      'modes': ['text', 'code', 'tree', 'form', 'view'],
      'mode': mode,
      'search': True,
    }
    if options:
      default_options.update(options)

    self.options = default_options
    self.width = width
    self.height = height

    super(GroutEditorWidget, self).__init__(attrs=attrs)

  def get_context(self, name, value, attrs):
    context = super().get_context(name, value, attrs)
    context['widget']['options'] = json.dumps(self.options)
    context['widget']['width'] = self.width
    context['widget']['height'] = self.height

    return context
