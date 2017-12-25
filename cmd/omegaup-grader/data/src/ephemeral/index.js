'use strict';

import JSZip from 'jszip';
import Vue from 'vue';
import Vuex from 'vuex';

import * as Util from './util';
import CaseSelectorComponent from './CaseSelectorComponent.vue';
import MonacoDiffComponent from './MonacoDiffComponent.vue';
import MonacoEditorComponent from './MonacoEditorComponent.vue';
import SettingsComponent from './SettingsComponent.vue';
import TextEditorComponent from './TextEditorComponent.vue';
import ZipViewerComponent from './ZipViewerComponent.vue';

Vue.use(Vuex);
let store = new Vuex.Store({
  state: {
    request: {
      input: {
        limits: {},
      },
    },
    results: null,
    outputs: {},
    currentCase: '',
    logs: '',
    compilerOutput: '',
  },
  getters: {
    moduleName(state) {
      if (state.request.input.interactive) {
        return state.request.input.interactive.module_name;
      }
      return 'Main';
    },
    flatCaseResults(state) {
      let result = {};
      if (!state.results || !state.results.groups) return result;
      for (let group of state.results.groups) {
        for (let caseData of group.cases) {
          result[caseData.name] = caseData;
        }
      }
      return result;
    },
    currentCase(state) {
      return state.currentCase;
    },
    inputIn(state) {
      return state.request.input.cases[state.currentCase]['in'];
    },
    inputOut(state) {
      return state.request.input.cases[state.currentCase]['out'];
    },
    outputStdout(state) {
      let filename = state.currentCase + '.out';
      if (!state.outputs[filename]) {
        return '';
      }
      return state.outputs[filename];
    },
    outputStderr(state) {
      let filename = state.currentCase + '.err';
      if (!state.outputs[filename]) {
        return '';
      }
      return state.outputs[filename];
    },
    'request.language'(state) {
      return state.request.language;
    },
    'request.input.validator.custom_validator.language'(state) {
      if (!state.request.input.validator.custom_validator) return '';
      return state.request.input.validator.custom_validator.language;
    },
    'request.input.validator.custom_validator.source'(state) {
      if (!state.request.input.validator.custom_validator) return '';
      return state.request.input.validator.custom_validator.source;
    },
    'request.input.interactive.idl'(state) {
      if (!state.request.input.interactive) return '';
      return state.request.input.interactive.idl;
    },
    'request.input.interactive.main_source'(state) {
      if (!state.request.input.interactive) return '';
      return state.request.input.interactive.main_source;
    },
    isCustomValidator(state) {
      return !!state.request.input.validator.custom_validator;
    },
    isInteractive(state) {
      return !!state.request.input.interactive;
    },
  },
  mutations: {
    currentCase(state, value) {
      state.currentCase = value;
    },
    compilerOutput(state, value) {
      state.compilerOutput = value;
    },
    logs(state, value) {
      state.logs = value;
    },
    request(state, value) {
      Vue.set(state, 'request', value);
    },
    'request.language'(state, value) {
      state.request.language = value;
    },
    'request.source'(state, value) {
      state.request.source = value;
    },
    inputIn(state, value) {
      state.request.input.cases[state.currentCase]['in'] = value;
    },
    inputOut(state, value) {
      state.request.input.cases[state.currentCase].out = value;
    },
    results(state, value) {
      Vue.set(state, 'results', value);
    },
    clearOutputs(state) {
      Vue.set(state, 'outputs', {});
    },
    output(state, payload) {
      Vue.set(state.outputs, payload.name, payload.contents);
    },
    'request.input.validator.custom_validator.source'(state, value) {
      if (!state.request.input.validator.custom_validator) return;
      state.request.input.validator.custom_validator.source = value;
    },
    'request.input.interactive.idl'(state, value) {
      if (!state.request.input.interactive) return;
      state.request.input.interactive.idl = value;
    },
    'request.input.interactive.main_source'(state, value) {
      if (!state.request.input.interactive) return;
      state.request.input.interactive.main_source = value;
    },

    TimeLimit(state, value) {
      state.request.input.limits.TimeLimit = value;
    },
    OverallWallTimeLimit(state, value) {
      state.request.input.limits.OverallWallTimeLimit = value;
    },
    ExtraWallTime(state, value) {
      state.request.input.limits.ExtraWallTime = value;
    },
    MemoryLimit(state, value) {
      state.request.input.limits.MemoryLimit = value;
    },
    OutputLimit(state, value) {
      state.request.input.limits.OutputLimit = value;
    },
    Validator(state, value) {
      if (value == 'token-numeric') {
        if (!state.request.input.validator.hasOwnProperty('tolerance'))
          Vue.set(state.request.input.validator, 'tolerance', 1e-9);
      } else {
        Vue.delete(state.request.input.validator, 'tolerance');
      }
      if (value == 'custom') {
        if (!state.request.input.validator.hasOwnProperty('custom_validator')) {
          Vue.set(state.request.input.validator, 'custom_validator', {
            source: `#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function

import logging
import sys

def _main():
  # lee "data.in" para obtener la entrada original.
  with open('data.in', 'r') as f:
    a, b = [int(x) for x in f.read().strip().split()]
  suma = a + b

  score = 0
  try:
    # Lee la salida del concursante
    suma_concursante = int(raw_input().strip())

    # Determina si la salida es correcta
    if suma_concursante == suma:
      score = 1
    else:
      # Cualquier cosa que imprimas a sys.stderr se ignora, pero es útil
      # para depurar con debug-rejudge.
      print('Salida incorrecta', file=sys.stderr)
  except Exception as e:
    log.exception('Error leyendo la salida del concursante')
  finally:
    print(score)

if __name__ == '__main__':
  _main()`,
            language: 'py',
          });
        }
      } else {
        Vue.delete(state.request.input.validator, 'custom_validator');
      }
      state.request.input.validator.name = value;
    },
    Tolerance(state, value) {
      state.request.input.validator.tolerance = value;
    },
    ValidatorLanguage(state, value) {
      state.request.input.validator.custom_validator.language = value;
    },
    Interactive(state, value) {
      if (value) {
        Vue.set(state.request.input, 'interactive', {
          idl: `interface Main {
};

interface sumas {
    int sumas(int a, int b);
};`,
          module_name: 'sumas',
          language: 'cpp11',
          main_source: `#include <stdio.h>
#include "sumas.h"

int main(int argc, char* argv[]) {
    int a, b;
    scanf("%d %d\\n", &a, &b);
    printf("%d\\n", sumas(a, b));
}`,
        });
      } else {
        Vue.delete(state.request.input, 'interactive');
      }
    },
    InteractiveLanguage(state, value) {
      state.request.input.interactive.language = value;
    },
    InteractiveModuleName(state, value) {
      state.request.input.interactive.module_name = value;
    },

    createCase(state, name) {
      if (state.request.input.cases.hasOwnProperty(name)) return;
      Vue.set(state.request.input.cases, name, {
        in: '',
        out: '',
        weight: 1,
      });
    },
    removeCase(state, name) {
      if (!state.request.input.cases.hasOwnProperty(name)) return;
      if (name == 'sample') return;
      if (name == state.currentCase) state.currentCase = 'sample';
      Vue.delete(state.request.input.cases, name);
    },
    reset(state) {
      Vue.set(state, 'request', {
        source:
          "#include <iostream>\n\nint main() {\n\tint64_t a, b;\n\tstd::cin >> a >> b;\n\tstd::cout << a + b << '\\n';\n}",
        language: 'cpp11',
        input: {
          limits: {
            TimeLimit: 1000, // 1s
            MemoryLimit: 67108864, // 64MB
            OverallWallTimeLimit: 5000, // 5s
            ExtraWallTime: 0, // 0s
            OutputLimit: 10240, // 10k
          },
          validator: {
            name: 'token-caseless',
          },
          cases: {
            sample: {
              in: '1 2\n',
              out: '3\n',
              weight: 1,
            },
            long: {
              in: '123456789012345678 123456789012345678\n',
              out: '246913578024691356\n',
              weight: 1,
            },
          },
          interactive: undefined,
        },
      });
      state.result = null;
      Vue.set(state, 'outputs', {});
      state.currentCase = 'sample';
      state.logs = '';
      state.compilerOutput = '';
    },
  },
  strict: true,
});
store.commit('reset');

const goldenLayoutSettings = {
  settings: {
    showPopoutIcon: false,
  },
  content: [
    {
      type: 'row',
      content: [
        {
          type: 'column',
          content: [
            {
              type: 'stack',
              id: 'sourceAndSettings',
              content: [
                {
                  type: 'component',
                  componentName: 'monaco-editor-component',
                  componentState: {
                    storeMapping: {
                      contents: 'request.source',
                      language: 'request.language',
                      module: 'moduleName',
                    },
                  },
                  id: 'source',
                  isClosable: false,
                },
                {
                  type: 'component',
                  componentName: 'settings-component',
                  componentState: {
                    storeMapping: {},
                    id: 'settings',
                  },
                  isClosable: false,
                },
              ],
            },
            {
              type: 'stack',
              content: [
                {
                  type: 'component',
                  componentName: 'text-editor-component',
                  componentState: {
                    storeMapping: {
                      contents: 'compilerOutput',
                    },
                    id: 'compiler',
                    readOnly: true,
                    initialModule: 'compiler',
                    extension: 'out/err',
                  },
                  isClosable: false,
                },
                {
                  type: 'component',
                  componentName: 'text-editor-component',
                  componentState: {
                    storeMapping: {
                      contents: 'logs',
                    },
                    id: 'logs',
                    readOnly: true,
                    initialModule: 'logs',
                    extension: 'txt',
                  },
                  isClosable: false,
                },
                {
                  type: 'component',
                  componentName: 'zip-viewer-component',
                  componentState: {
                    storeMapping: {},
                    id: 'zipviewer',
                  },
                  title: 'files.zip',
                  isClosable: false,
                },
              ],
              height: 20,
            },
          ],
        },
        {
          type: 'column',
          content: [
            {
              type: 'row',
              content: [
                {
                  type: 'component',
                  componentName: 'text-editor-component',
                  componentState: {
                    storeMapping: {
                      contents: 'inputIn',
                      module: 'currentCase',
                    },
                    id: 'in',
                    readOnly: false,
                    extension: 'in',
                  },
                  isClosable: false,
                },
                {
                  type: 'component',
                  componentName: 'text-editor-component',
                  componentState: {
                    storeMapping: {
                      contents: 'inputOut',
                      module: 'currentCase',
                    },
                    id: 'out',
                    readOnly: false,
                    extension: 'out',
                  },
                  isClosable: false,
                },
              ],
            },
            {
              type: 'stack',
              content: [
                {
                  type: 'component',
                  componentName: 'text-editor-component',
                  componentState: {
                    storeMapping: {
                      contents: 'outputStdout',
                      module: 'currentCase',
                    },
                    id: 'stdout',
                    readOnly: false,
                    extension: 'out',
                  },
                  isClosable: false,
                },
                {
                  type: 'component',
                  componentName: 'text-editor-component',
                  componentState: {
                    storeMapping: {
                      contents: 'outputStderr',
                      module: 'currentCase',
                    },
                    id: 'stderr',
                    readOnly: false,
                    extension: 'err',
                  },
                  isClosable: false,
                },
                {
                  type: 'component',
                  componentName: 'monaco-diff-component',
                  componentState: {
                    storeMapping: {
                      originalContents: 'inputOut',
                      modifiedContents: 'outputStdout',
                    },
                    id: 'diff',
                  },
                  isClosable: false,
                },
              ],
            },
          ],
        },
        {
          type: 'component',
          componentName: 'case-selector-component',
          componentState: {
            storeMapping: {
              cases: 'request.input.cases',
              currentCase: 'currentCase',
            },
            id: 'source',
          },
          title: 'cases/',
          width: 15,
          isClosable: false,
        },
      ],
    },
  ],
};
const validatorSettings = {
  type: 'component',
  componentName: 'monaco-editor-component',
  componentState: {
    storeMapping: {
      contents: 'request.input.validator.custom_validator.source',
      language: 'request.input.validator.custom_validator.language',
    },
    initialModule: 'validator',
  },
  id: 'validator',
  isClosable: false,
};
const interactiveIdlSettings = {
  type: 'component',
  componentName: 'monaco-editor-component',
  componentState: {
    storeMapping: {
      contents: 'request.input.interactive.idl',
      module: 'request.input.interactive.module_name',
    },
    initialLanguage: 'idl',
  },
  id: 'interactive_idl',
  isClosable: false,
};
const interactiveMainSourceSettings = {
  type: 'component',
  componentName: 'monaco-editor-component',
  componentState: {
    storeMapping: {
      contents: 'request.input.interactive.main_source',
      language: 'request.input.interactive.language',
    },
    initialModule: 'Main',
  },
  id: 'interactive_main_source',
  isClosable: false,
};

const layout = new GoldenLayout(
  goldenLayoutSettings,
  document.getElementById('layout-root')
);

function RegisterVueComponent(layout, componentName, component, componentMap) {
  layout.registerComponent(componentName, function(container, componentState) {
    container.on('open', () => {
      let vueComponents = {};
      vueComponents[componentName] = component;
      let props = {
        store: store,
        storeMapping: componentState.storeMapping,
      };
      for (let k in componentState) {
        if (k == 'id') continue;
        if (!componentState.hasOwnProperty(k)) continue;
        props[k] = componentState[k];
      }
      let vue = new Vue({
        el: container.getElement()[0],
        render: function(createElement) {
          return createElement(componentName, {
            props: props,
          });
        },
        components: vueComponents,
      });
      let vueComponent = vue.$children[0];
      if (vueComponent.title) {
        container.setTitle(vueComponent.title);
        vueComponent.$watch('title', function(title) {
          container.setTitle(title);
        });
      }
      if (vueComponent.onResize) {
        container.on('resize', () => vueComponent.onResize());
      }
      componentMap[componentState.id] = vueComponent;
    });
  });
}

let componentMapping = {};
RegisterVueComponent(
  layout,
  'case-selector-component',
  CaseSelectorComponent,
  componentMapping
);
RegisterVueComponent(
  layout,
  'monaco-editor-component',
  MonacoEditorComponent,
  componentMapping
);
RegisterVueComponent(
  layout,
  'monaco-diff-component',
  MonacoDiffComponent,
  componentMapping
);
RegisterVueComponent(
  layout,
  'settings-component',
  SettingsComponent,
  componentMapping
);
RegisterVueComponent(
  layout,
  'text-editor-component',
  TextEditorComponent,
  componentMapping
);
RegisterVueComponent(
  layout,
  'zip-viewer-component',
  ZipViewerComponent,
  componentMapping
);

layout.init();

let sourceAndSettings = layout.root.getItemsById('sourceAndSettings')[0];
if (store.getters.isCustomValidator)
  sourceAndSettings.addChild(validatorSettings);
store.watch(
  Object.getOwnPropertyDescriptor(store.getters, 'isCustomValidator').get,
  function(value) {
    if (value) sourceAndSettings.addChild(validatorSettings);
    else layout.root.getItemsById(validatorSettings.id)[0].remove();
  }
);
if (store.getters.isInteractive) {
  sourceAndSettings.addChild(interactiveIdlSettings);
  sourceAndSettings.addChild(interactiveMainSourceSettings);
}
store.watch(
  Object.getOwnPropertyDescriptor(store.getters, 'isInteractive').get,
  function(value) {
    if (value) {
      sourceAndSettings.addChild(interactiveIdlSettings);
      sourceAndSettings.addChild(interactiveMainSourceSettings);
    } else {
      layout.root.getItemsById(interactiveIdlSettings.id)[0].remove();
      layout.root.getItemsById(interactiveMainSourceSettings.id)[0].remove();
    }
  }
);

if (window.ResizeObserver) {
  new ResizeObserver(() => {
    layout.updateSize();
  }).observe(document.getElementById('layout-root'));
} else {
  window.addEventListener('resize', () => {
    layout.updateSize();
  });
}

document.getElementById('language').addEventListener('change', function() {
  store.commit('request.language', this.value);
});
store.watch(
  Object.getOwnPropertyDescriptor(store.getters, 'request.language').get,
  function(value) {
    document.getElementById('language').value = value;
  }
);

function onDetailsJsonReady(results) {
  store.commit('results', results);
  store.commit('compilerOutput', results.compile_error || '');
}

function onFilesZipReady(blob) {
  if (blob == null || blob.size == 0) {
    if (componentMapping.zipviewer) {
      componentMapping.zipviewer.zip = null;
    }
    store.commit('clearOutputs');
    return;
  }
  let reader = new FileReader();
  reader.addEventListener('loadend', () => {
    JSZip.loadAsync(reader.result).then(zip => {
      if (componentMapping.zipviewer) {
        componentMapping.zipviewer.zip = zip;
      }
      store.commit('clearOutputs');
      Promise.all([
        zip.file('Main/compile.err').async('string'),
        zip.file('Main/compile.out').async('string'),
      ]).then(values => {
        for (let value of values) {
          if (!value) continue;
          store.commit('compilerOutput', value);
          return;
        }
        store.commit('compilerOutput', '');
      });
      for (let filename in zip.files) {
        if (filename.indexOf('/') !== -1) continue;
        zip
          .file(filename)
          .async('string')
          .then(contents => {
            store.commit('output', {
              name: filename,
              contents: contents,
            });
          });
      }
    });
  });
  reader.readAsArrayBuffer(blob);
}

document.getElementsByTagName('form')[0].addEventListener('submit', e => {
  e.preventDefault();
  document.getElementsByTagName('button')[0].setAttribute('disabled', '');
  fetch('run/new/', {
    method: 'POST',
    headers: new Headers({
      'Content-Type': 'application/json',
    }),
    body: JSON.stringify(store.state.request),
  })
    .then(response => {
      if (!response.ok) return null;
      history.replaceState(
        undefined,
        undefined,
        '#' + response.headers.get('X-OmegaUp-EphemeralToken')
      );
      return response.formData();
    })
    .then(formData => {
      document.getElementsByTagName('button')[0].removeAttribute('disabled');
      if (!formData) {
        onDetailsJsonReady({
          verdict: 'JE',
          contest_score: 0,
          max_score: 100,
        });
        store.commit('logs', '');
        onFilesZipReady(null);
        return;
      }

      if (formData.has('details.json')) {
        let reader = new FileReader();
        reader.addEventListener('loadend', function() {
          onDetailsJsonReady(JSON.parse(reader.result));
        });
        reader.readAsText(formData.get('details.json'));
      }

      if (formData.has('logs.txt.gz')) {
        let reader = new FileReader();
        reader.addEventListener('loadend', function() {
          if (reader.result.byteLength == 0) {
            store.commit('logs', '');
            return;
          }

          store.commit(
            'logs',
            new TextDecoder('utf-8').decode(pako.inflate(reader.result))
          );
        });
        reader.readAsArrayBuffer(formData.get('logs.txt.gz'));
      } else {
        store.commit('logs', '');
      }

      onFilesZipReady(formData.get('files.zip'));
    });
});

function onHashChanged() {
  if (window.location.hash.length == 0) {
    store.commit('reset');
    store.commit('logs', '');
    onDetailsJsonReady({});
    onFilesZipReady(null);
    return;
  }

  let token = window.location.hash.substring(1);
  fetch('run/' + token + '/request.json')
    .then(response => {
      if (!response.ok) return null;
      return response.json();
    })
    .then(request => {
      if (!request) {
        store.commit('reset');
        store.commit('logs', '');
        onDetailsJsonReady({});
        onFilesZipReady(null);
        return;
      }
      store.commit('request', request);
      fetch('run/' + token + '/details.json')
        .then(response => {
          if (!response.ok) return {};
          return response.json();
        })
        .then(onDetailsJsonReady);
      fetch('run/' + token + '/files.zip')
        .then(response => {
          if (!response.ok) return null;
          return response.blob();
        })
        .then(onFilesZipReady);
      fetch('run/' + token + '/logs.txt')
        .then(response => {
          if (!response.ok) return '';
          return response.text();
        })
        .then(text => store.commit('logs', text));
    });
}
window.addEventListener('hashchange', onHashChanged, false);
onHashChanged();
