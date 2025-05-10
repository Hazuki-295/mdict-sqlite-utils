import fs from 'fs';
import path from 'path';

import Database from 'better-sqlite3';
import workerpool from 'workerpool';

import { ContentType } from './types.js';
import type { MdxRecord, MdxDatabaseOptions } from './types.js';

async function transformHtmlRecords(records: MdxRecord[], modulePath: string): Promise<MdxRecord[]> {
    // Dynamically import and cache the transformHtml function
    if (typeof globalThis.transformHtml === 'undefined' || globalThis.transformModulePath !== modulePath) {
        const { transformHtml } = await import(modulePath) as { transformHtml: (html: string) => string };

        if (typeof transformHtml !== 'function') {
            throw new Error('Module must export a function with the signature: transformHtml(html: string): string');
        }

        globalThis.transformHtml = transformHtml;
        globalThis.transformModulePath = modulePath;
    }

    // eslint-disable-next-line @typescript-eslint/no-unsafe-call
    return records.map(record => ({ ...record, paraphrase: globalThis.transformHtml(record.paraphrase) as string }));
}

export default class MdxDatabase {
    private static instances: Set<MdxDatabase> = new Set<MdxDatabase>();
    private db: Database.Database;

    constructor(filename: string, options?: MdxDatabaseOptions) {
        this.db = new Database(filename, options?.options);
        MdxDatabase.instances.add(this);

        if (options?.initialize) this.initialize();
        if (options?.optimize) this.optimize();
    }

    public static closeAll() {
        for (const instance of MdxDatabase.instances) {
            instance.close();
        }
    }

    public static splitRecordsByContentType(records: MdxRecord[]): { htmlRecords: MdxRecord[]; linkRecords: MdxRecord[] } {
        const result = { htmlRecords: [] as MdxRecord[], linkRecords: [] as MdxRecord[] };
        for (const record of records) {
            if (record.content_type === ContentType.HTML) result.htmlRecords.push(record);
            else if (record.content_type === ContentType.LINK) result.linkRecords.push(record);
        }
        return result;
    }

    public static async writeRecordsToFiles(records: MdxRecord[], outputDir: string, transformModulePath?: string) {
        // Ensure output directory exists
        fs.mkdirSync(outputDir, { recursive: true });

        const { htmlRecords, linkRecords } = MdxDatabase.splitRecordsByContentType(records);

        // Transform HTML records if a transformation module is provided
        const processedHtmlRecords = transformModulePath
            ? await transformHtmlRecords(htmlRecords, path.resolve(transformModulePath))
            : htmlRecords;

        processedHtmlRecords.forEach(record => {
            const filePath = path.join(outputDir, `${record.entry}_${record.rowid}.html`);
            fs.writeFileSync(filePath, record.paraphrase);
            console.log(`Written HTML record: ${filePath}`);
        });

        linkRecords.forEach(record => {
            const filePath = path.join(outputDir, `${record.entry}_${record.rowid}.txt`);
            fs.writeFileSync(filePath, record.paraphrase);
            console.log(`Written LINK record: ${filePath}`);
        });
    }

    private static async transformHtmlRecordsInParallel(htmlRecords: MdxRecord[], pool: workerpool.Pool, transformModulePath: string, chunkSize = 1e3): Promise<MdxRecord[]> {
        // Split HTML records into chunks
        const chunks: MdxRecord[][] = [];
        for (let i = 0; i < htmlRecords.length; i += chunkSize) {
            chunks.push(htmlRecords.slice(i, i + chunkSize));
        }

        // Transform chunks of HTML records in parallel
        const transformedChunks = await Promise.all(
            chunks.map(chunk => pool.exec(transformHtmlRecords, [chunk, path.resolve(transformModulePath)]))
        );
        const transformedRecords = transformedChunks.flat();

        return transformedRecords;
    }

    public close() {
        this.db.close();
        MdxDatabase.instances.delete(this);
    }

    public getTotalRecordCount(contentType?: ContentType): number {
        const stmt = contentType !== undefined
            ? this.db.prepare('SELECT COUNT(*) AS count FROM mdx WHERE content_type = ?')
            : this.db.prepare('SELECT COUNT(*) AS count FROM mdx');

        const { count } = contentType !== undefined
            ? stmt.get(contentType) as { count: number }
            : stmt.get() as { count: number };

        return count;
    }

    public fetchRecordsByEntries(entries: string[]): MdxRecord[] {
        const uniqueEntries = [...new Set(entries)];

        const placeholders = uniqueEntries.map(() => '?').join(', ');
        const stmt = this.db.prepare(`SELECT entry, paraphrase, content_type, rowid FROM mdx WHERE entry IN (${placeholders})`);
        const records = stmt.all(...uniqueEntries) as MdxRecord[];

        const foundEntries = new Set(records.map(record => record.entry));
        const notFoundEntries = uniqueEntries.filter(entry => !foundEntries.has(entry));
        notFoundEntries.forEach(entry => {
            console.warn(`warning: no record found for entry: ${entry}`);
        });

        return records;
    }

    public fetchRecords(contentType?: ContentType): MdxRecord[] {
        if (contentType !== undefined) {
            const stmt = this.db.prepare('SELECT entry, paraphrase, content_type, rowid FROM mdx WHERE content_type = ? ORDER BY rowid');
            return stmt.all(contentType) as MdxRecord[];
        } else {
            const stmt = this.db.prepare('SELECT entry, paraphrase, content_type, rowid FROM mdx ORDER BY rowid');
            return stmt.all() as MdxRecord[];
        }
    }

    public fetchLinkRecords(): MdxRecord[] {
        return this.fetchRecords(ContentType.LINK);
    }

    public fetchHtmlRecords(): MdxRecord[] {
        return this.fetchRecords(ContentType.HTML);
    }

    public * fetchRecordsPaginated(contentType?: ContentType, pageSize = 1e4): Generator<MdxRecord[]> {
        const stmt = contentType !== undefined
            ? this.db.prepare('SELECT entry, paraphrase, content_type, rowid FROM mdx WHERE content_type = ? AND rowid > ? ORDER BY rowid LIMIT ?')
            : this.db.prepare('SELECT entry, paraphrase, content_type, rowid FROM mdx WHERE rowid > ? ORDER BY rowid LIMIT ?');

        let records: MdxRecord[] = [];
        let lastRowId = 0;

        do {
            records = contentType !== undefined
                ? stmt.all(contentType, lastRowId, pageSize) as MdxRecord[]
                : stmt.all(lastRowId, pageSize) as MdxRecord[];

            if (records.length === 0) break;
            yield records;
            lastRowId = records[records.length - 1].rowid!;
        } while (true);
    }

    public fetchLinkRecordsPaginated(pageSize = 1e4): Generator<MdxRecord[]> { // not used
        return this.fetchRecordsPaginated(ContentType.LINK, pageSize);
    }

    public fetchHtmlRecordsPaginated(pageSize = 1e4): Generator<MdxRecord[]> {
        return this.fetchRecordsPaginated(ContentType.HTML, pageSize);
    }

    public async * processRecordsInParallel(transformModulePath: string, pageSize = 1e4, chunkSize = 1e3): AsyncGenerator<MdxRecord[]> {
        const pool = workerpool.pool();

        try {
            for (const records of this.fetchRecordsPaginated(undefined, pageSize)) {
                const htmlRecords = records.filter(record => record.content_type === ContentType.HTML);
                const transformedRecords = await MdxDatabase.transformHtmlRecordsInParallel(htmlRecords, pool, transformModulePath, chunkSize);

                for (let i = 0; i < htmlRecords.length; i++) {
                    htmlRecords[i].paraphrase = transformedRecords[i].paraphrase;
                }

                yield records;
            }
        } finally {
            await pool.terminate();
        }
    }

    public async * transformHtmlRecordsInParallel(transformModulePath: string, pageSize = 1e4, chunkSize = 1e3): AsyncGenerator<MdxRecord[]> {
        const pool = workerpool.pool();

        try {
            for (const htmlRecords of this.fetchHtmlRecordsPaginated(pageSize)) {
                const transformedRecords = await MdxDatabase.transformHtmlRecordsInParallel(htmlRecords, pool, transformModulePath, chunkSize);
                yield transformedRecords;
            }
        } finally {
            await pool.terminate();
        }
    }

    public insertRecords(records: MdxRecord[]) {
        const stmt = this.db.prepare('INSERT INTO mdx (entry, paraphrase, content_type) VALUES (?, ?, ?)');
        const transaction = this.db.transaction((records: MdxRecord[]) => {
            for (const record of records) {
                record.content_type ??= record.paraphrase.startsWith('@@@LINK=') ? ContentType.LINK : ContentType.HTML;
                stmt.run(record.entry, record.paraphrase, record.content_type);
            }
        });
        transaction(records);
    }

    public deleteRecords(entries: string[]) {
        const placeholders = entries.map(() => '?').join(', ');
        const stmt = this.db.prepare(`DELETE FROM mdx WHERE entry IN (${placeholders})`);
        stmt.run(...entries);
    }

    private initialize() {
        this.db.prepare('DROP TABLE IF EXISTS mdx').run();
        this.db.prepare('CREATE TABLE mdx (entry TEXT NOT NULL, paraphrase TEXT NOT NULL, content_type INTEGER NOT NULL)').run();
    }

    private optimize() {
        // Check if the 'content_type' column already exists in the 'mdx' table
        const columns = this.db.prepare('PRAGMA table_info(mdx)').all() as { name: string }[];
        const hasContentTypeColumn = columns.some(column => column.name === 'content_type');

        if (!hasContentTypeColumn) {
            // Open the database in read-write mode to allow schema modifications
            const db = new Database(this.db.name);

            // Add a new column 'content_type' to categorize the mdx records
            db.prepare('ALTER TABLE mdx ADD COLUMN content_type INTEGER').run();

            // Set the 'content_type' based on the 'paraphrase' field pattern
            db.prepare("UPDATE mdx SET content_type = CASE WHEN paraphrase LIKE '@@@LINK=%' THEN ? ELSE ? END").run(ContentType.LINK, ContentType.HTML);

            // Create an index on the 'content_type' column to speed up queries
            db.prepare('CREATE INDEX mdx_content_type_index ON mdx (content_type)').run();

            // Close the temporary connection
            db.close();
        }
    }
}
