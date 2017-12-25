export function vuexGet(store, name) {
  if (typeof store.getters[name] !== 'undefined') return store.getters[name];
  var o = store.state;
  for (let p of name.split('.')) {
    if (!o.hasOwnProperty(p)) return undefined;
    o = o[p];
  }
  return o;
}

export function vuexSet(store, name, value) {
  store.commit(name, value);
}

export const languageMonacoModelMapping = {
  cpp11: 'cpp',
  cs: 'csharp',
  java: 'java',
  lua: 'lua',
  py: 'python',
  rb: 'ruby',

  // Fake languages.
  idl: 'text',
  in: 'text',
  out: 'text',
  err: 'text',
};

export const languageExtensionMapping = {
  cpp11: 'cpp',
  cs: 'cs',
  java: 'java',
  lua: 'lua',
  py: 'py',
  rb: 'rb',

  // Fake languages.
  idl: 'idl',
  in: 'in',
  out: 'out',
  err: 'err',
};
