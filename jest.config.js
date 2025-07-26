import "dotenv/config";

export default {
  preset: "ts-jest",
  testEnvironment: "node",
  setupFiles: ["dotenv/config"],
  testTimeout: 300000,
  coveragePathIgnorePatterns: ["tests/integration/test-utils.ts"],
};
