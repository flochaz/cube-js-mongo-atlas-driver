/* eslint-disable import/no-extraneous-dependencies */
/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `MongoAtlasJDBC` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
  CancelablePromise,
} from '@cubejs-backend/shared';
import fs from 'fs';
import path from 'path';

import * as SqlString from 'sqlstring';
import {
  BaseDriver,
  SchemaStructure,
  TableStructure,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
} from '@cubejs-backend/base-driver';
import { promisify } from 'util';
import { v4 as uuidv4 } from 'uuid';

import genericPool, { Factory, Pool } from 'generic-pool';
import { MongoAtlasJDBCQuery } from './MongoAtlasJDBCQuery';
import { DriverConfiguration } from './types';

const mvn = require('node-java-maven');
const jinst = require('@cubejs-backend/jdbc/lib/jinst');
const DatabaseMetaData = require('@cubejs-backend/jdbc/lib/databasemetadata');
const ResultSetMetaData = require('@cubejs-backend/jdbc/lib/resultsetmetadata');
const DriverManager = require('@cubejs-backend/jdbc/lib/drivermanager');
const QueryStream = require('@cubejs-backend/jdbc-driver/dist/src/QueryStream');

export type MongoAtlasConfiguration = DriverConfiguration;

const Connection = require('@cubejs-backend/jdbc/lib/connection');

ResultSetMetaData.prototype.getColumnTypeName = function (
  column: any,
  callback: any
) {
  this._rsmd.getColumnTypeName(column, (err: any, typeName: any) => {
    try {
      if (err) {
        return callback(err);
      } else {
        return callback(null, typeName);
      }
    } catch (error) {
      return callback(error);
    }
  });
};

function fileExistsOr(fsPath: string, fn: () => string) {
  if (fs.existsSync(fsPath)) {
    return fsPath;
  }
  return fn();
}

function resolveJDBCDriver(): string {
  // Get the driver path from the environment variable, if it exists
  return fileExistsOr(path.join(process.cwd(), 'MongoAtlasJDBC.jar'), () => {
    const driverPath = path.join(
      __dirname,
      '..',
      'download',
      'MongoAtlasJDBC.jar'
    );
    if (fs.existsSync(driverPath)) {
      return driverPath;
    }
    throw new Error('MongoAtlasJDBC.jar not found');
  });
}

let mvnPromise: Promise<any> | null = null;
const initMvn = (customClassPath: string) => {
  if (!mvnPromise) {
    mvnPromise = new Promise((resolve, reject) => {
      const options = {
        packageJsonPath: `${path.join(__dirname, '../..')}/package.json`,
      };
      mvn(options, (err: any, mvnResults: any) => {
        if (err && !err.message.includes('Could not find java property')) {
          reject(err);
        } else {
          if (!jinst.isJvmCreated()) {
            jinst.addOption('-Xrs');
            jinst.addOption('-Dfile.encoding=UTF8');
            const classPath = (
              (mvnResults && mvnResults.classpath) ||
              []
            ).concat(customClassPath || []);
            jinst.setupClasspath(classPath);
          }
          resolve('OK');
        }
      });
    });
  }
  return mvnPromise;
};

const applyParams = (query: string, params: object | any[]) => SqlString.format(query, params);

// promisify Connection methods
Connection.prototype.getMetaDataAsync = promisify(
  Connection.prototype.getMetaData
);
// promisify DatabaseMetaData methods
DatabaseMetaData.prototype.getSchemasAsync = promisify(
  DatabaseMetaData.prototype.getSchemas
);
DatabaseMetaData.prototype.getTablesAsync = promisify(
  DatabaseMetaData.prototype.getTables
);

interface ExtendedPool extends Pool<any> {
  _factory: Factory<any>;
}

export class MongoAtlasJDBCDriver extends BaseDriver {
  private _id: string;

  protected readonly config: DriverConfiguration;

  protected pool: ExtendedPool;

  protected props: any;

  private jdbcProps: any;

  public constructor(
    config: Partial<DriverConfiguration> & {
      /**
       * Data source name.
       */
      dataSource?: string;

      /**
       * Max pool size value for the [cube]<-->[db] pool.
       */
      maxPoolSize?: number;

      /**
       * Time to wait for a response from a connection after validation
       * request before determining it as not valid. Default - 60000 ms.
       */
      testConnectionTimeout?: number;
    } = {}
  ) {
    super({
      testConnectionTimeout: config.testConnectionTimeout || 60000,
    });

    const dataSource = config.dataSource || assertDataSource('default');

    const url: string = config?.url || getEnv('jdbcUrl', { dataSource });

    const { poolOptions, ...dbOptions } = config;

    this.config = {
      dbType: 'mongo-atlas-jdbc',
      url,
      drivername: 'com.mongodb.jdbc.MongoDriver',
      // TODO: Check if that does not block multi data source support
      properties: {
        user: process.env.CUBEJS_DB_USER,
        password: process.env.CUBEJS_DB_PASS,
        port: process.env.CUBEJS_DB_PORT,
      },
      ...dbOptions,
    } as DriverConfiguration;

    if (!this.config.drivername) {
      throw new Error('drivername is required property');
    }

    if (!this.config.url) {
      throw new Error('url is required property');
    }

    console.log('creating pool ....');
    this.pool = genericPool.createPool(
      {
        create: async () => {
          try {
            console.log('creating connection ...');
            await initMvn(await this.getCustomClassPath());
            if (!this.jdbcProps) {
              this.jdbcProps = this.getJdbcProperties();
            }
            const getConnection = promisify(
              DriverManager.getConnection.bind(DriverManager)
            );
            const connection = new Connection(
              await getConnection(this.config.url, this.jdbcProps)
            );
            console.log('Connection created successfully', {
              connectionId: connection.id,
            }); // Assuming connections have an id for logging
            return connection;
          } catch (error) {
            console.log('Connection creation failed', {
              error: (error as Error).message,
            });
            throw error; // Ensure the error is not swallowed
          }
        },
        destroy: async (connection) => {
          try {
            console.log('destroying connection ...');
            await promisify(connection.close.bind(connection))();
            console.log('Connection destroyed successfully', {
              connectionId: connection.id,
            });
          } catch (error: any) {
            console.log('Connection destruction failed', {
              connectionId: connection.id,
              error: error.message,
            });
          }
        },
        validate: async (connection) => new Promise((resolve) => {
          console.log('validating connection ...');
          const isValid = promisify(connection.isValid.bind(connection));
          const timeout = setTimeout(() => {
            console.log('Connection validation failed by timeout', {
              connectionId: connection.id,
              testConnectionTimeout: this.testConnectionTimeout(),
            });
            resolve(false);
          }, this.testConnectionTimeout());
          isValid(0)
            .then((valid: boolean) => {
              console.log('Connection validation succeeded', {
                connectionId: connection.id,
                valid,
              });
              clearTimeout(timeout);
              if (!valid) {
                console.log('Connection validation failed', {
                  connectionId: connection.id,
                });
              }
              resolve(valid);
            })
            .catch((e: Error) => {
              clearTimeout(timeout);
              console.log('Connection validation error', {
                connectionId: connection.id,
                error: e.stack || e.message,
              });
              this.databasePoolError(e);
              resolve(false);
            });
        }),
      },
      {
        min: 0,
        max: config.maxPoolSize || getEnv('dbMaxPoolSize', { dataSource }) || 3,
        evictionRunIntervalMillis: 10000,
        softIdleTimeoutMillis: 30000,
        idleTimeoutMillis: 30000,
        testOnBorrow: true,
        acquireTimeoutMillis: 120000,
        ...(poolOptions || {}),
      }
    ) as ExtendedPool;

    this._id = uuidv4();

    console.log(this._id, `creating driver for: ${JSON.stringify(config)}`);
  }

  public static dialectClass() {
    return MongoAtlasJDBCQuery;
  }

  public override readOnly() {
    return true;
  }

  private async getCustomClassPath() {
    return resolveJDBCDriver();
  }

  private getJdbcProperties() {
    const java = jinst.getInstance();
    const Properties = java.import('java.util.Properties');
    const properties = new Properties();
    for (const [name, value] of Object.entries(this.config.properties)) {
      properties.putSync(name, value);
    }
    return properties;
  }

  public async testConnection() {
    let err;
    let connection;
    try {
      connection = await this.pool._factory.create();
    } catch (e: any) {
      err = e.message || e;
    }
    if (err) {
      throw new Error(err.toString());
    } else {
      await this.pool._factory.destroy(connection);
    }
  }

  protected prepareConnectionQueries() {
    return this.config.prepareConnectionQueries;
  }

  public async query<R = unknown>(
    query: string,
    values: unknown[]
  ): Promise<R[]> {
    try {
      const resultSet = await this.queryRaw(query, values);
      try {
        if (!(resultSet as { toObjArray?: () => Promise<any[]> }).toObjArray) {
          throw new Error('Result set does not have a toObjArray method.');
        }
        const toObjArrayAsync = promisify(
          (
            resultSet as unknown as { toObjArray: () => Promise<any[]> }
          ).toObjArray.bind(resultSet)
        );
        const rows = await toObjArrayAsync();
        return rows as R[]; // Explicitly typecast rows to R[]
      } finally {
        const closeResultSet = promisify(
          (resultSet as unknown as { close: () => Promise<void> }).close.bind(
            resultSet
          )
        );
        await closeResultSet();
      }
    } catch (error) {
      console.error('An error occurred during the query operation:', error);
      throw error;
    }
  }

  private async queryRaw<R = unknown>(
    query: string,
    values: unknown[]
  ): Promise<R[]> {
    const queryWithParams = applyParams(query, values);
    const cancelObj: { cancel?: Function } = {};
    const promise = this.queryPromised(
      queryWithParams,
      cancelObj,
      this.prepareConnectionQueries()
    );
    (promise as CancelablePromise<any>).cancel = () => (cancelObj.cancel && cancelObj.cancel()) ||
      Promise.reject(new Error('Statement is not ready'));
    return promise;
  }

  public async withConnection(fn: (conn: any) => Promise<any>) {
    const conn = await this.pool.acquire();
    try {
      return await fn(conn);
    } finally {
      await this.pool.release(conn);
    }
  }

  protected async queryPromised(
    query: string,
    cancelObj: any,
    options: any = {}
  ): Promise<any> {
    try {
      const conn = await this.pool.acquire();
      try {
        const prepareConnectionQueries = options.prepareConnectionQueries || [];
        for (let i = 0; i < prepareConnectionQueries.length; i++) {
          await this.executeStatement(conn, prepareConnectionQueries[i]);
        }
        return await this.executeStatement(conn, query, cancelObj);
      } finally {
        await this.pool.release(conn);
      }
    } catch (ex: any) {
      if (ex.cause) {
        throw new Error(ex.cause.getMessageSync());
      } else {
        throw ex;
      }
    }
  }

  public async streamQuery(
    sql: string,
    values: any[]
  ): Promise<typeof QueryStream> {
    console.log(
      this._id,
      'streaming query for:',
      this.config.database,
      'poolsize: ',
      this.pool.size,
      JSON.stringify(sql)
    );
    const conn = await this.pool.acquire();
    const query = applyParams(sql, values);
    const cancelObj: { cancel?: Function } = {};
    try {
      const createStatement = promisify(conn.createStatement.bind(conn));
      const statement = await createStatement();
      if (cancelObj) {
        cancelObj.cancel = promisify(statement.cancel.bind(statement));
      }
      const executeQuery = promisify(statement.execute.bind(statement));
      const resultSet = await executeQuery(query);
      return new Promise((resolve, reject) => {
        resultSet.toObjectIter((err: Error, res: any) => {
          if (err) reject(err);
          const rowsStream = new QueryStream(res.rows.next);
          let connectionReleased = false;
          const cleanup = (e: Error) => {
            if (!connectionReleased) {
              this.pool.release(conn);
              connectionReleased = true;
            }
            if (!rowsStream.destroyed) {
              rowsStream.destroy(e);
            }
          };
          rowsStream.once('end', cleanup);
          rowsStream.once('error', cleanup);
          rowsStream.once('close', cleanup);
          resolve(rowsStream);
        });
      });
    } catch (ex: any) {
      await this.pool.release(conn);
      if (ex.cause) {
        throw new Error(ex.cause.getMessageSync());
      } else {
        throw ex;
      }
    }
  }

  protected async executeStatement(
    conn: any,
    query: string,
    cancelObj?: any
  ): Promise<any> {
    console.log(
      this._id,
      'executing query for:',
      this.config.database,
      'poolsize: ',
      this.pool.size,
      JSON.stringify(query)
    );
    const createStatementAsync = promisify(conn.createStatement.bind(conn));
    const statement: any = await createStatementAsync();
    if (cancelObj) {
      cancelObj.cancel = promisify(statement.cancel.bind(statement));
    }
    const setQueryTimeout = promisify(
      statement.setQueryTimeout.bind(statement)
    );
    await setQueryTimeout(600);
    const executeQueryAsync = promisify(statement.execute.bind(statement));
    const resultSet = await executeQueryAsync(query);
    return resultSet;
  }

  public async release() {
    console.log('releasing pool ....');
    await this.pool.drain();
    await this.pool.clear();
  }

  public async tableColumnTypes(table: string): Promise<TableStructure> {
    const [database, name] = table.split('.');
    const conn = await this.pool.acquire();
    try {
      const getMetaDataAsync = promisify(conn.getMetaData.bind(conn));
      const schema: TableStructure = await getMetaDataAsync().then(
        async (metadata: any) => {
          const getColumns = promisify(metadata.getColumns.bind(metadata));
          const columnsResults = await getColumns(null, database, name, null);
          const toObjArrayColumns = promisify(
            columnsResults.toObjArray.bind(columnsResults)
          );
          const columnsResultsArray = await toObjArrayColumns();
          console.log('columnsResultsArray', JSON.stringify(columnsResultsArray));
          return columnsResultsArray.map((field: any) => ({ name: <string>field.COLUMN_NAME, type: this.toGenericType(field.TYPE_NAME || 'text') }));
          // const columnTypes: any = [];
          // columnsResultsArray.forEach((columnRow: any) => {
          //   columnTypes.push({
          //     name: columnRow.COLUMN_NAME,
          //     type: this.toGenericType(columnRow.TYPE_NAME),
          //   });
          // });
          // return columnTypes;
        }
      );
      return schema;
    } catch (err) {
      console.error(err);
      throw err;
    } finally {
      await this.pool.release(conn);
    }
  }

  public override async tablesSchema(): Promise<SchemaStructure> {
    const conn = await this.pool.acquire();
    try {
      const getMetaDataAsync = promisify(conn.getMetaData.bind(conn));
      const schema = await getMetaDataAsync().then(async (metadata: any) => {
        const subSchema: any = {};
        const getTables = promisify(metadata.getTables.bind(metadata));
        const tableResults = await getTables(
          process.env.CUBEJS_DB_NAME,
          null,
          null,
          ['TABLE']
        );
        const toObjArray = promisify(
          tableResults.toObjArray.bind(tableResults)
        );
        const tableResultsArray = await toObjArray();
        for (const tableRow of tableResultsArray) {
          const getColumns = promisify(metadata.getColumns.bind(metadata));
          const columnsResults = await getColumns(
            null,
            tableRow.TABLE_SCHEM,
            tableRow.TABLE_NAME,
            null
          );
          const subTableSchema: any = {}; // Explicitly define the type of tableSchema
          const toObjArrayColumns = promisify(
            columnsResults.toObjArray.bind(columnsResults)
          );
          const columnsResultsArray = await toObjArrayColumns();
          columnsResultsArray.forEach((columnRow: any) => {
            const tables = subTableSchema[columnRow.TABLE_NAME] || [];
            tables.push({
              name: columnRow.COLUMN_NAME,
              type: columnRow.TYPE_NAME,
              attributes: columnRow.KEY_TYPE ? ['primaryKey'] : [],
            });
            tables.sort();
            subTableSchema[columnRow.TABLE_NAME] = tables;
          });
          subSchema[tableRow.TABLE_CAT] = subTableSchema;
        }
        return subSchema;
      });
      return schema;
    } catch (err) {
      console.error(err);
      throw err;
    } finally {
      await this.pool.release(conn);
    }
  }

  public override async downloadQueryResults(query: string, values: unknown[], _options: DownloadQueryResultsOptions): Promise<DownloadQueryResultsResult> {
    const resultSet: any = await this.queryRaw(query, values);
    const toObjArrayAsync = resultSet.toObjArray && promisify(resultSet.toObjArray.bind(resultSet));
    const rows = await toObjArrayAsync();
    const getMetaDataAsync = promisify(resultSet.getMetaData.bind(resultSet));
    const metaData = await getMetaDataAsync();
    const types = [];
    const getColumnCount = promisify(metaData.getColumnCount.bind(metaData));
    for (let i = 1; i <= await getColumnCount(); i++) {
      const getColumnName = promisify(metaData.getColumnName.bind(metaData));
      const getColumnTypeName = promisify(metaData.getColumnTypeName.bind(metaData));
      types.push({
        name: await getColumnName(i),
        type: this.toGenericType(await getColumnTypeName(i))
      });
    }
    return {
      rows,
      types,
    };
  }

  public override toGenericType(columnType: string) {
    if (columnType === 'null') {
      return 'text';
    }
    if (columnType === 'objectId') {
      return 'string';
    }
    if (columnType === 'bson') {
      return 'text';
    }
    if (columnType === 'timestamp') {
      return 'date';
    }
    return super.toGenericType(columnType);
  }
}
