// The LLDB module provides low-level database access functionality and defines
// the interfaces which must be conformed to by LLDB drivers.

(Symbol as any).asyncIterator = Symbol.asyncIterator || Symbol.for('Symbol.asyncIterator');

export interface IContext {}

const _registry: {[driverName: string]: ILLDBDriver} = {};

// Registers a new driver. Throws if a driver with the same name has already
// been registered.
export function registerDriver(driverName: string, driver: ILLDBDriver) {
  if (_registry[driverName])
    throw new Error(`SQL driver already registered: "${driverName}"`);

  _registry[driverName] = driver;
}

// Connects using the specified driver. The driverArgs are in a driver-specific
// format. Throws if the driver with the specified name is not found.
export function connect(ctx: IContext, driverName: string, driverArgs: any): Promise<ILLDBConnection> {
  const driver = _registry[driverName];
  if (!driver)
    throw new Error(`unknown SQL driver: "${driverName}"`);

  return driver.connect(ctx, driverArgs);
}

export interface ILLDBDriver {
  connect(ctx: IContext, connectArgs: any): Promise<ILLDBConnection>;
}

export interface ILLDBConnection {
  // Tears down the connection. Calling this multiple times is inconsequential.
  // Calling any other method after calling this will result in an error.
  close(): Promise<void>;

  // Attempts to verify that the connection is still functional. May be a no-op
  // in some implementations.
  ping(ctx: IContext): Promise<void>;

  // Begin a transaction.
  begin(ctx: IContext, options: ILLDBConnectionTxOptions): Promise<ILLDBConnectionTx>;

  // Execute an SQL command, ignoring any result sets. Preferred for SQL
  // commands which don't have result sets.
  exec(ctx: IContext, sqlText: string, ...args: any[]): Promise<ILLDBResult>;

  // Execute an SQL command and provide access to its result sets.
  query(ctx: IContext, sqlText: string, ...args: any[]): Promise<ILLDBRows>;

  // If true, the connection is currently engaged due to a returned query
  // ILLDBRows object which has yet to be concluded (done === true, whether via
  // close() or via exhausting the iterator). Calling ping, begin, exec or
  // query will result in an error being thrown.
  readonly engaged: boolean;

  // If true, an explicit transaction is currently in progress, meaning calls
  // to begin() will fail.
  readonly txEngaged: boolean;
}

/*export async function withQuery<T>(ctx: IContext, conn: ILLDBConnection, sqlText: string, args: any[], f: (rows: ILLDBRows) => Promise<T>): Promise<T> {
 const rows = await conn.query(sqlText, ...args);
 try {
   return await f(rows);
 } finally {
   rows.close();
 }
}*/

export enum IsolationLevel {
  Default,
  ReadUncommitted,
  ReadCommitted,
  WriteCommitted,
  RepeatableRead,
  Snapshot,
  Serializable,
  Linearizable,
}

// Error thrown whenever a database-side error (e.g. a SQL syntax error) is
// received over the connection.
export class LLDBError extends Error {
}

export interface ILLDBConnectionTxOptions {
  isolationLevel?: IsolationLevel;
  readOnly?: boolean;
}

export interface ILLDBConnectionTx {
  // Commits the transaction. Any call to commit or rollback commencing after
  // the call to commit commences is ignored.
  commit(): Promise<void>;

  // Safe to call multiple times, even if another call to rollback or commit
  // has not completed.
  rollback(): Promise<void>;
}

export interface ILLDBResult {
  lastInsertID?: number;
  rowsAffected?: number;
  tag?: string;
}

export interface ILLDBRows extends AsyncIterableIterator<any[]> {
  close(): Promise<void>;
  readonly columns: string[];
  readonly done: boolean; // all result sets received?
  readonly tag: string | null; // command completion tag; null if not completed or not supported

  [Symbol.asyncIterator](): this; // return this; exception if this.done
}
