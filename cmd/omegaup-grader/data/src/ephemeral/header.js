(function() {
  require.config({
    paths: {
      vs: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.10.1/min/vs',
      jszip: 'https://cdnjs.cloudflare.com/ajax/libs/jszip/3.1.5/jszip',
      pako: 'https://cdnjs.cloudflare.com/ajax/libs/pako/1.0.6/pako.min.js',
    },
  });
  window.MonacoEnvironment = {
    getWorkerUrl: function (workerId, label) {
      return 'monaco-editor-worker-loader-proxy.js';
    },
  };
  require(['jszip', 'pako', 'vs/editor/editor.main'], function (JSZip, pako) {
