import Database from 'better-sqlite3';

enum ContentType { HTML, LINK }

interface MdxRecord {
    entry: string;
    paraphrase: string;
    content_type: ContentType;
    rowid?: number | undefined;
}

interface MdxDatabaseOptions {
    initialize?: boolean | undefined;
    optimize?: boolean | undefined;
    options?: Database.Options | undefined;
}

interface TransformOptions {
    htmlBeforeLink?: boolean | undefined;
}

export { ContentType, MdxRecord, MdxDatabaseOptions, TransformOptions };
