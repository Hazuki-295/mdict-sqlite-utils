import fs from 'fs';

import cliProgress from 'cli-progress';

import MdxDatabase from './database.js';
import { ContentType } from './types.js';
import type { TransformOptions } from './types.js';

function validateModuleExists(modulePath: string) {
    if (!fs.existsSync(modulePath)) {
        throw new Error(`Module not found at path: ${modulePath}`);
    }
}

function createProgressBar(label: string): cliProgress.SingleBar {
    return new cliProgress.SingleBar({
        format: `${label} {bar} {percentage}% | Elapsed Time: {duration_formatted} | {value}/{total}`, align: 'center', hideCursor: true
    }, cliProgress.Presets.shades_classic);
}

export async function runTransformWorkflow(sourceDatabase: MdxDatabase, targetDatabase: MdxDatabase, transformModulePath: string, options?: TransformOptions) {
    console.log('→ transform workflow');
    console.log(`• module: ${transformModulePath}`);
    validateModuleExists(transformModulePath);

    if (options?.htmlBeforeLink) {
        console.log('• mode: HTML before LINK');

        const progressBar = createProgressBar('Transforming HTML records');
        progressBar.start(sourceDatabase.getTotalRecordCount(ContentType.HTML), 0);

        try {
            for await (const transformedRecords of sourceDatabase.transformHtmlRecordsInParallel(transformModulePath)) {
                targetDatabase.insertRecords(transformedRecords);
                progressBar.increment(transformedRecords.length);
            }
        } finally {
            progressBar.stop();
        }

        console.log('✓ HTML records inserted');

        targetDatabase.insertRecords(sourceDatabase.fetchLinkRecords());
        console.log('✓ LINK records inserted');
    } else {
        const progressBar = createProgressBar('Processing records');
        progressBar.start(sourceDatabase.getTotalRecordCount(), 0);

        try {
            for await (const processedRecords of sourceDatabase.processRecordsInParallel(transformModulePath)) {
                targetDatabase.insertRecords(processedRecords);
                progressBar.increment(processedRecords.length);
            }
        }
        finally {
            progressBar.stop();
        }
    }
    console.log('✓ transform workflow complete');
}

export async function runDebugWorkflow(database: MdxDatabase, entries: string[], outputDir: string, transformModulePath?: string) {
    console.log('→ debug mode');
    if (transformModulePath) {
        console.log(`• module: ${transformModulePath}`);
        validateModuleExists(transformModulePath);
    }

    const records = database.fetchRecordsByEntries(entries);
    if (records.length === 0) {
        console.log('✓ no records found');
    } else {
        console.log(`✓ found ${records.length} records`);
        await MdxDatabase.writeRecordsToFiles(records, outputDir, transformModulePath);
    }
}

export function runStatsWorkflow(database: MdxDatabase, statsModulePath: string) {
    console.log('→ stats mode');
    console.log(`• module: ${statsModulePath}`);
    validateModuleExists(statsModulePath);

    console.log('✓ not implemented');
}
