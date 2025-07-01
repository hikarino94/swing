module.exports = {
  testEnvironment: 'jsdom',
  testMatch: ['**/static/js/**/*.test.js', '**/static/js/test_*.js'],
  collectCoverageFrom: [
    'static/js/**/*.js',
    '!static/js/test_*.js',
    '!static/js/**/*.test.js'
  ],
  coverageDirectory: 'coverage-js',
  coverageReporters: ['text', 'lcov', 'html'],
  setupFilesAfterEnv: ['<rootDir>/jest.setup.js']
};
