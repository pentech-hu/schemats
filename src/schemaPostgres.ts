import * as PgPromise from 'pg-promise'
import { mapValues } from 'lodash'
import { keys } from 'lodash'
import Options from './options'

import { TableDefinition, Database } from './schemaInterfaces'

const pgp = PgPromise()

export class PostgresDatabase implements Database {
    private db: PgPromise.IDatabase<{}>

    constructor (public connectionString: string) {
        this.db = pgp(connectionString)
    }

    private static mapTableDefinitionToType (tableDefinition: TableDefinition, customTypes: string[], options: Options): TableDefinition {
        return mapValues(tableDefinition, column => {
            switch (column.udtName) {
                case 'bpchar':
                case 'char':
                case 'varchar':
                case 'text':
                case 'citext':
                case 'uuid':
                case 'bytea':
                case 'inet':
                case 'time':
                case 'timetz':
                case 'interval':
                case 'name':
                    column.tsType = 'string'
                    return column
                case 'int2':
                case 'int4':
                case 'int8':
                case 'float4':
                case 'float8':
                case 'numeric':
                case 'money':
                case 'oid':
                    column.tsType = 'number'
                    return column
                case 'bool':
                    column.tsType = 'boolean'
                    return column
                case 'json':
                case 'jsonb':
                    column.tsType = 'Object'
                    return column
                case 'date':
                case 'timestamp':
                case 'timestamptz':
                    column.tsType = 'Date'
                    return column
                case '_int2':
                case '_int4':
                case '_int8':
                case '_float4':
                case '_float8':
                case '_numeric':
                case '_money':
                    column.tsType = 'Array<number>'
                    return column
                case '_bool':
                    column.tsType = 'Array<boolean>'
                    return column
                case '_varchar':
                case '_text':
                case '_citext':                    
                case '_uuid':
                case '_bytea':
                    column.tsType = 'Array<string>'
                    return column
                case '_json':
                case '_jsonb':
                    column.tsType = 'Array<Object>'
                    return column
                case '_timestamptz':
                    column.tsType = 'Array<Date>'
                    return column
                default:
                    if (customTypes.indexOf(column.udtName) !== -1) {
                        column.tsType = options.transformTypeName(column.udtName)
                        return column
                    } else {
                        console.log(`Type [${column.udtName} has been mapped to [any] because no specific type has been found.`)
                        column.tsType = 'any'
                        return column
                    }
            }
        })
    }

    public query (queryString: string) {
        return this.db.query(queryString)
    }

    public async getEnumTypes (schema?: string) {
        type T = {name: string, value: any}
        let enums: any = {}
        let enumSchemaWhereClause = schema ? pgp.as.format(`where n.nspname = $1`, schema) : ''
        await this.db.each<T>(
             'select n.nspname as schema, t.typname as name, e.enumlabel as value ' +
             'from pg_type t ' +
             'join pg_enum e on t.oid = e.enumtypid ' +
             'join pg_catalog.pg_namespace n ON n.oid = t.typnamespace ' +
             `${enumSchemaWhereClause} ` +
             'order by t.typname asc, e.enumlabel asc;', [],
            (item: T) => {
                if (!enums[item.name]) {
                    enums[item.name] = []
                }
                enums[item.name].push(item.value)
            }
        )
        return enums
    }

    public async getTableDefinition (tableName: string, tableSchema: string) {
        let tableDefinition: TableDefinition = {}
        type T = { column_name: string, udt_name: string, is_nullable: boolean }
        await this.db.each<T>(
            `
            SELECT
                pg_attribute.attname AS column_name,
                pg_type.typname::information_schema.sql_identifier AS udt_name,
                pg_attribute.attnotnull AS is_nullable
            FROM
                pg_attribute
                JOIN pg_class ON pg_attribute.attrelid = pg_class.oid
                JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                JOIN pg_type ON pg_type.oid = pg_attribute.atttypid
            WHERE
                pg_attribute.attnum > 0
                AND NOT pg_attribute.attisdropped
                AND pg_class.relname = $1
                AND pg_namespace.nspname = $2
            ORDER BY
                pg_attribute.attnum;
            `.split('\n').map(line => line.trim()).join(' '),
            [tableName, tableSchema],
            (schemaItem: T) => {
                tableDefinition[schemaItem.column_name] = {
                    udtName: schemaItem.udt_name,
                    nullable: schemaItem.is_nullable === false
                }
            })
        return tableDefinition
    }

    public async getTableTypes (tableName: string, tableSchema: string, options: Options) {
        let enumTypes = await this.getEnumTypes()
        let customTypes = keys(enumTypes)
        return PostgresDatabase.mapTableDefinitionToType(await this.getTableDefinition(tableName, tableSchema), customTypes, options)
    }

    public async getSchemaTables (schemaName: string): Promise<string[]> {
        return await this.db.map<string>(
            'SELECT relname AS table_name ' +
            'FROM pg_class ' +
            'INNER JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace ' +
            'WHERE pg_namespace.nspname = $1 ' +
            `AND relkind IN ('m', 'v', 'r') ` + // mat view, view, ordinary table
            'GROUP BY relname ' +
            'ORDER BY relname',
            [schemaName],
            (schemaItem: {table_name: string}) => schemaItem.table_name
        )
    }

    getDefaultSchema (): string {
        return 'public'
    }
}
