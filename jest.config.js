module.exports = {
  moduleFileExtensions: ['js', 'json', 'ts'],
  rootDir: '.',
  testRegex: '.*\\.spec\\.ts$',
  transform: {
    '^.+\\.(t|j)s$': [
      '@swc/jest',
      {
        jsc: {
          parser: {
            syntax: 'typescript',
            decorators: true,
            dynamicImport: true,
          },
          transform: {
            legacyDecorator: true,
            decoratorMetadata: true,
          },
          target: 'es2022',
          loose: true,
          keepClassNames: true,
        },
        module: {
          type: 'commonjs',
          importInterop: 'none',
        },
      },
    ],
  },
  collectCoverageFrom: [
    'src/**/*.(t|j)s',
    '!src/**/*.spec.ts',
    '!src/**/*.interface.ts',
    '!src/index.ts',
  ],
  coverageDirectory: './coverage',
  testEnvironment: 'node',
  testTimeout: 10000,
  moduleNameMapper: {
    '^@torix/nestjs-kafka/(.*)$': '<rootDir>/src/$1',
  },
  setupFiles: ['<rootDir>/jest.swc-setup.js'],
  setupFilesAfterEnv: ['<rootDir>/test/setup.ts'],
  testPathIgnorePatterns: ['integration'],
};
