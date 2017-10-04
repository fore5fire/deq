var webpack = require('webpack');
const path = require('path');
//const nodeExternals = require('webpack-node-externals');

module.exports = {
  entry: './src/index.js',
  target: 'node',
  output: {
    filename: 'bundle.js',
    path: path.resolve('dist'),
    libraryTarget: 'commonjs2',
  },
  //externals: [nodeExternals()],
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
