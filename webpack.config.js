var webpack = require('webpack');
const path = require('path');
const fs = require('fs');

const target = 'authenticator.js';


const nodeModules = {};
fs.readdirSync('node_modules')
  .filter(function(x) {
    return ['.bin'].indexOf(x) === -1;
  })
  .forEach(function(mod) {
    nodeModules[mod] = 'commonjs ' + mod;
  });

module.exports = {
  entry: './src/index.js',
  target: 'node',
  output: {
    filename: target,
    path: path.resolve('dist')
  },
  externals: nodeModules,
  module: {
    rules: [
      { test: /\.(txt|graphql)$/, use: 'raw-loader', },
      { test: /\.js$/, use: 'babel-loader'}
    ],

  },
  plugins: [
    new webpack.BannerPlugin({
      banner: 'require("source-map-support").install();',
      raw: true,
      entryOnly: false
    })
  ],
  devtool: 'sourcemap',
};
