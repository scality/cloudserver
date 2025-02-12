import mocha from "eslint-plugin-mocha";
import path from "node:path";
import { fileURLToPath } from "node:url";
import js from "@eslint/js";
import { FlatCompat } from "@eslint/eslintrc";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const compat = new FlatCompat({
    baseDirectory: __dirname,
    recommendedConfig: js.configs.recommended,
    allConfig: js.configs.all
});

export default [...compat.extends('@scality/scality'), {
    plugins: {
        mocha,
    },

    languageOptions: {
        ecmaVersion: 2020,
        sourceType: "script",
    },

    rules: {
        "import/extensions": "off",
        "lines-around-directive": "off",
        "no-underscore-dangle": "off",
        "indent": "off",
        "object-curly-newline": "off",
        "operator-linebreak": "off",
        "function-paren-newline": "off",
        "import/newline-after-import": "off",
        "prefer-destructuring": "off",
        "implicit-arrow-linebreak": "off",
        "no-bitwise": "off",
        "dot-location": "off",
        "comma-dangle": "off",
        "no-undef-init": "off",
        "global-require": "off",
        "import/no-dynamic-require": "off",
        "class-methods-use-this": "off",
        "no-plusplus": "off",
        "no-else-return": "off",
        "object-property-newline": "off",
        "import/order": "off",
        "no-continue": "off",
        "no-tabs": "off",
        "lines-between-class-members": "off",
        "prefer-spread": "off",
        "no-lonely-if": "off",
        "no-useless-escape": "off",
        "no-restricted-globals": "off",
        "no-buffer-constructor": "off",
        "import/no-extraneous-dependencies": "off",
        "space-unary-ops": "off",
        "no-useless-return": "off",
        "no-unexpected-multiline": "off",
        "no-mixed-operators": "off",
        "newline-per-chained-call": "off",
        "operator-assignment": "off",
        "spaced-comment": "off",
        "comma-style": "off",
        "no-restricted-properties": "off",
        "new-parens": "off",
        "no-multi-spaces": "off",
        "quote-props": "off",
        "mocha/no-exclusive-tests": "error",
        "no-redeclare": ["error", { "builtinGlobals": false }],
    },
}];


