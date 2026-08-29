module.exports = function (api) {
  api.cache(true);
  // babel-preset-expo covers all three platforms: it swaps in react-native-web's
  // module aliases when the caller platform is web, and the RN preset otherwise.
  return { presets: ["babel-preset-expo"] };
};
