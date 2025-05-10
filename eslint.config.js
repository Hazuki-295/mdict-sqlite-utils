import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import stylistic from '@stylistic/eslint-plugin'

export default tseslint.config(
    eslint.configs.recommended,
    tseslint.configs.recommendedTypeChecked,
    tseslint.configs.stylisticTypeChecked,
    {
        plugins: {
            '@stylistic': stylistic
        },
        rules: {
            "@typescript-eslint/explicit-member-accessibility": ["error", { overrides: { constructors: "no-public" } }],
            "@typescript-eslint/member-ordering": "error",
            "@typescript-eslint/no-empty-function": ["error", { allow: ["private-constructors"] }],
            "@typescript-eslint/no-unused-vars": "warn",
            "@typescript-eslint/unbound-method": ["error", { ignoreStatic: true }],
            "@stylistic/generator-star-spacing": ["error", { "before": true, "after": true }]
        },
        languageOptions: { parserOptions: { projectService: true } },
        ignores: ["dist/*", "eslint.config.js"]
    },
);
