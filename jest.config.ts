import fs from 'fs';
import type {Config} from 'jest';
import path from 'path';

function getProjectConfig(projectPath: string) {
  const jestConfig = JSON.parse(
    fs.readFileSync(`${projectPath}/package.json`, 'utf8')
  ).jest;
  // check transform key and fix the tsconfig path
  if (jestConfig.transform) {
    for (const key in jestConfig.transform) {
      if (jestConfig.transform[key]?.[1]?.tsconfig?.startsWith('test/')) {
        jestConfig.transform[key][1].tsconfig =
          `<rootDir>/${jestConfig.transform[key][1].tsconfig}`;
      }
    }
  }
  return {
    displayName: path.basename(projectPath),
    rootDir: path.resolve(__dirname, projectPath),
    ...jestConfig,
  };
}

const config: Config = {
  verbose: true,
  projects: [
    getProjectConfig('faros-airbyte-cdk'),
    // https://github.com/jestjs/jest/issues/6155
    getProjectConfig('faros-airbyte-common'),
    getProjectConfig('sources/klaviyo-source'),
  ],
  // roots: ['<rootDir>/faros-airbyte-common'],
};

export default config;
