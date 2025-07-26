import pluginJs from "@eslint/js";
import prettierConfig from "eslint-config-prettier";
import eslintPluginPrettierRecommended from "eslint-plugin-prettier/recommended";
import simpleImportSort from "eslint-plugin-simple-import-sort";
import cdkPlugin from "eslint-cdk-plugin";
import tseslint from "@typescript-eslint/eslint-plugin";
import tsparser from "@typescript-eslint/parser";

export default [
  {
    ignores: ["cdk.out/", "node_modules/"],
  },
  {
    files: ["**/*.js"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
      globals: {
        module: "readonly",
      },
    },
    plugins: {
      "simple-import-sort": simpleImportSort,
    },
    rules: {
      "simple-import-sort/imports": "error",
      "simple-import-sort/exports": "error",
    },
  },
  {
    files: ["**/*.ts"],
    languageOptions: {
      parser: tsparser,
      parserOptions: {
        project: "./tsconfig.json",
        tsconfigRootDir: import.meta.dirname,
      },
      globals: {
        afterEach: "readonly",
        beforeEach: "readonly",
        Buffer: "readonly",
        console: "readonly",
        describe: "readonly",
        expect: "readonly",
        jest: "readonly",
        it: "readonly",
        process: "readonly",
        setTimeout: "readonly",
        test: "readonly",
      },
    },
    plugins: {
      "@typescript-eslint": tseslint,
      "simple-import-sort": simpleImportSort,
      cdk: cdkPlugin,
    },
    rules: {
      ...tseslint.configs.recommended.rules,
      ...cdkPlugin.configs.recommended.rules,
      "simple-import-sort/imports": "error",
      "simple-import-sort/exports": "error",
      "@typescript-eslint/no-explicit-any": "off",
    },
  },
  eslintPluginPrettierRecommended,
  pluginJs.configs.recommended,
  prettierConfig,
];
