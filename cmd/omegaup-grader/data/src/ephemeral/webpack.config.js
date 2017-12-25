const fs = require('fs');
const path = require('path');
const webpack = require('webpack');
const UglifyJsPlugin = require('uglifyjs-webpack-plugin');
const WrapperPlugin = require('wrapper-webpack-plugin');

const header = fs.readFileSync('./header.js', 'utf8');
const footer = fs.readFileSync('./footer.js', 'utf8');

module.exports = {
  entry: ['babel-polyfill', './index.js'],
  externals: {
    jszip: 'raw JSZip',
    monaco: 'raw monaco',
    pako: 'raw pako',
    tr: 'raw tr',
  },
  module: {
    rules: [
      {
        test: /\.vue$/,
        loader: 'vue-loader',
        options: {
          loaders: {},
          // other vue-loader options go here
        },
      },
      {
        test: /\.js$/,
        exclude: /node_modules/,
        use: {
          loader: 'babel-loader',
          options: {
            presets: ['env'],
          },
        },
      },
    ],
  },
  resolve: {
    alias: {
      vue$: 'vue/dist/vue.common.js',
      'vue-async-computed': 'vue-async-computed/dist/index.js',
    },
  },
  plugins: [
    new WrapperPlugin({
      test: /index\.js$/,
      header: header,
      footer: footer,
    }),
  ],
  output: {
    filename: 'index.js',
    path: path.resolve(__dirname, '../../dist/ephemeral'),
  },
  devtool: '#cheap-source-map',
};

if (process.env.NODE_ENV === 'production') {
  module.exports.devtool = '#source-map';
  // http://vue-loader.vuejs.org/en/workflow/production.html
  module.exports.plugins = (module.exports.plugins || []).concat([
    new webpack.DefinePlugin({
      'process.env': {
        NODE_ENV: '"production"',
      },
    }),
    new UglifyJsPlugin({
      sourceMap: true,
    }),
    new webpack.LoaderOptionsPlugin({
      minimize: true,
    }),
  ]);
}
