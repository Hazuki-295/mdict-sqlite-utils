#!/usr/bin/env node

import path from 'path';

import { Command } from '@commander-js/extra-typings';

import MdxDatabase from './database.js';
import * as workflows from './workflows.js';

import packageJson from '../package.json' with { type: "json" };

const program = new Command();

program
    .name(packageJson.name)
    .version(packageJson.version)
    .description(packageJson.description);

program
    .command('transform <transform-module-path>')
    .description('Process all records using the specified transformation module')
    .requiredOption('-d, --db <path>', 'path to input database')
    .option('-o, --output <path>', 'path to output database (default: "./output.db" in same directory as input, will overwrite if exists)')
    .option('-s, --html-before-link', 'place all HTML records before LINK records, preserving relative order within each type')
    .action(async (transformModulePath, options) => {
        const sourceDatabase = new MdxDatabase(options.db, { optimize: true, options: { readonly: true } });
        const targetDatabase = new MdxDatabase(options.output ?? path.join(path.dirname(options.db), 'output.db'), { initialize: true });

        await workflows.runTransformWorkflow(sourceDatabase, targetDatabase, transformModulePath, { htmlBeforeLink: options.htmlBeforeLink });
    });

program
    .command('debug <keys...>')
    .description('Process specific records by keys and save them to files')
    .requiredOption('-d, --db <path>', 'path to input database')
    .option('-o, --output-dir <path>', 'directory to save output files (default: "./debug" in same directory as input)')
    .option('-t, --transform <transform-module-path>', 'path to transformation module (optional, will skip transformation if not provided)')
    .action(async (keys, options) => {
        const database = new MdxDatabase(options.db, { optimize: true, options: { readonly: true } });
        const outputDir = options.outputDir ?? path.join(path.dirname(options.db), 'debug');

        await workflows.runDebugWorkflow(database, keys, outputDir, options.transform);
    });

program
    .command('stats <stats-module-path>')
    .description('Generate statistics using the specified stats module')
    .requiredOption('-d, --db <path>', 'path to input database')
    .action((statsModulePath, options) => {
        const database = new MdxDatabase(options.db, { optimize: true, options: { readonly: true } });

        workflows.runStatsWorkflow(database, statsModulePath);
    });

process.on('SIGINT', () => {
    console.log('\nSIGINT signal received.');
    MdxDatabase.closeAll();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\nSIGTERM signal received.');
    MdxDatabase.closeAll();
    process.exit(0);
});

program.parseAsync().catch(console.error).finally(() => MdxDatabase.closeAll());
