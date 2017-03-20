import {IContext} from "hlandau.Context";
import {ILLDBConnection, ILLDBConnectionTxOptions, ILLDBConnectionTx, ILLDBResult, ILLDBRows, connect} from "hlandau.SQLAPI/LLDB";


/* Pool Interface
 * --------------
 */
export interface IPoolOptions {
  driverName: string;
  driverArgs: any;

  idleConnections?: number; // default 1
  maxConnections?:  number; // default unlimited
}

export type IDBTxOptions = ILLDBConnectionTxOptions;
export type IDBResult = ILLDBResult;
export type IDBRows = ILLDBRows;

export interface IDBBase {
  exec(ctx: IContext, sqlText: string, ...args: any[]): Promise<IDBResult>;
  query(ctx: IContext, sqlText: string, ...args: any[]): Promise<IDBRows>;
}

export interface IDB extends IDBBase {
  close(): Promise<void>;
  begin(ctx: IContext, options: IDBTxOptions): Promise<IDBTx>;
}

export interface IDBTx extends IDBBase {
  commit(): Promise<void>;
  rollback(): Promise<void>;
}


/* Pool Implementation
 * -------------------
 */

interface _ConnInfo {
  id: ConnID;
  conn: ILLDBConnection;
  pool: IDB;
  checkedOut: boolean;
  removed: boolean;
}
const symConnInfo = Symbol(); // ILLDBConnection[symConnInfo]: _ConnInfo

type ConnID = string;
let curConnID = 0;

class DBPoolRowsWrapper implements IDBRows {
  private __underlying: IDBRows;
  private __done: boolean = false;
  private __doneFunc: (() => void);

  constructor(rows: IDBRows, doneFunc: (() => void)) {
    this.__underlying = rows;
    this.__doneFunc = doneFunc;
  }

  async close(): Promise<void> {
    await this.__underlying.close();
    this.__checkStatus();
  }

  get columns(): string[] {
    return this.__underlying.columns;
  }

  get done(): boolean {
    return this.__underlying.done;
  }

  get tag(): string | null {
    return this.__underlying.tag;
  }

  [Symbol.asyncIterator](): this { return this; }

  next(): Promise<IteratorResult<any[]>> {
    const rv = this.__underlying.next();
    this.__checkStatus();
    return rv;
  }

  private __checkStatus() {
    if (this.__underlying.done && !this.__done) {
      this.__done = true;
      this.__doneFunc();
    }
  }
}

class DBPoolTxWrapper implements IDBTx {
  private __conn: ILLDBConnection;
  private __tx: ILLDBConnectionTx;
  private __returned: boolean = false;
  private __returnFunc: (() => void);

  constructor(conn: ILLDBConnection, tx: ILLDBConnectionTx, returnFunc: (() => void)) {
    this.__conn = conn;
    this.__tx   = tx;
    this.__returnFunc = returnFunc;
  }

  async commit(): Promise<void> {
    this.__check();

    await this.__tx.commit();
    this.__returnToPool();
  }

  // No-op if called after commit, rollback or failure.
  async rollback(): Promise<void> {
    if (this.__returned)
      return;

    await this.__tx.rollback();
    this.__returnToPool();
  }

  async exec(ctx: IContext, sqlText: string, ...args: any[]): Promise<IDBResult> {
    this.__check();
    return this.__conn.exec(ctx, sqlText, ...args);
  }

  async query(ctx: IContext, sqlText: string, ...args: any[]): Promise<IDBRows> {
    this.__check();
    return this.__conn.query(ctx, sqlText, ...args);
  }

  private __check() {
    if (this.__returned)
      throw new Error(`cannot use expired transaction object`);
  }

  private __returnToPool() {
    if (this.__returned)
      return;
    this.__returned = true;
    this.__returnFunc();
  }
}

class DBPool implements IDB {
  private __options: IPoolOptions;
  private __allConnections: {[connID: string]: _ConnInfo} = {};
  private __numConnections: number = 0;
  private __idleConnections: {[connID: string]: _ConnInfo} = {};
  private __numIdleConnections: number = 0;
  private __connectionAvailableSema = new Semaphore();
  private __deadError: Error | null = null;

  constructor(options: IPoolOptions) {
    this.__options = Object.assign({}, options);

    if (this.__options.idleConnections === undefined)
      this.__options.idleConnections = 1;

    if (this.__options.maxConnections !== undefined && this.__options.maxConnections < this.__options.idleConnections)
      this.__options.maxConnections = this.__options.idleConnections;
  }

  async close(): Promise<void> {
    if (this.__deadError !== null)
      return;

    this.__deadError = new Error(`database pool was closed`);
    this.__connectionAvailableSema.close();
  }

  private __checkDead() {
    if (this.__deadError)
      throw this.__deadError;
  }

  async begin(ctx: IContext, options: IDBTxOptions): Promise<IDBTx> {
    this.__checkDead();
    let ok = false;
    const conn = await this.__popConnection(ctx);
    try {
      const tx = await conn.begin(ctx, options);
      const txw = new DBPoolTxWrapper(conn, tx, () => {
        this.__returnConnection(conn);
      });
      ok = true;
      return txw;
    } finally {
      if (!ok)
        this.__returnConnection(conn);
    }
  }

  async exec(ctx: IContext, sqlText: string, ...args: any[]): Promise<IDBResult> {
    this.__checkDead();
    return await this.__withConnection(ctx, conn => {
      return conn.exec(ctx, sqlText, ...args);
    });
  }

  async query(ctx: IContext, sqlText: string, ...args: any[]): Promise<IDBRows> {
    this.__checkDead();
    let ok = false;
    const conn = await this.__popConnection(ctx);
    try {
      const rows = await conn.query(ctx, sqlText, ...args);
      const rw = new DBPoolRowsWrapper(rows, () => {
        this.__returnConnection(conn);
      });
      ok = true;
      return rw;
    } finally {
      if (!ok)
        this.__returnConnection(conn);
    }
  }

  private __addConnection(conn: ILLDBConnection) {
    const id = (++curConnID).toString();
    const info: _ConnInfo = {id, conn, pool: this, checkedOut: false, removed: false};
    (conn as any)[symConnInfo] = info;
    this.__allConnections[id] = info;
    this.__idleConnections[id] = info;
    ++this.__numConnections;
    ++this.__numIdleConnections;
  }

  private __removeConnection(info: _ConnInfo) {
    info.removed = true;
    if (this.__idleConnections[info.id])
      --this.__numIdleConnections;
    --this.__numConnections;
    delete(this.__idleConnections[info.id]);
    delete(this.__allConnections[info.id]);
    delete((info.conn as any)[symConnInfo]);
  }

  private async __popConnection(ctx: IContext): Promise<ILLDBConnection> {
    let conn = this.__tryPopConnection();
    if (conn)
      return conn;

    while (this.__options.maxConnections !== undefined && this.__numConnections >= this.__options.maxConnections && ctx.error === null) {
      await this.__connectionAvailableSema.wait();
      conn = this.__tryPopConnection();
      if (conn)
        return conn;
    }

    this.__checkDead();
    if (ctx.error)
      throw ctx.error;

    await this.__openAdditionalConnection(ctx);
    conn = this.__tryPopConnection();
    if (conn)
      return conn;

    throw new Error(`DB pool cannot open additional connection`);
  }

  private __tryPopConnection(): ILLDBConnection | null {
    let info: _ConnInfo | undefined = undefined;
    for (const id in this.__idleConnections) {
      info = this.__idleConnections[id];
      delete(this.__idleConnections[id]);
      --this.__numIdleConnections;
      break;
    }
    if (info === undefined)
      return null;

    if (info.checkedOut)
      throw new Error(`popped connection which is already checked out`);

    info.checkedOut = true;
    return info.conn;
  }

  private async __openAdditionalConnection(ctx: IContext): Promise<void> {
    const c = await this.__openConnection(ctx);
    this.__addConnection(c);
  }

  private __returnConnection(conn: ILLDBConnection) {
    const info = (conn as any)[symConnInfo] as _ConnInfo;
    if (typeof info !== 'object' || info.pool !== this)
      throw new Error(`cannot return DB connection which is not a member of the pool`);
    if (!info.checkedOut)
      throw new Error(`cannot return DB connection which is not checked out`);

    info.checkedOut = false;
    this.__idleConnections[info.id] = info;
    ++this.__numIdleConnections;
    this.__connectionAvailableSema.signal();
    this.__trimIdle();
  }

  private async __openConnection(ctx: IContext): Promise<ILLDBConnection> {
    if (ctx.error !== null)
      throw ctx.error;

    return await connect(ctx, this.__options.driverName, this.__options.driverArgs);
  }

  private __trimIdle() {
    while (this.__numIdleConnections > (this.__options.idleConnections || 0)) {
      for (const id in this.__idleConnections) {
        const info = this.__idleConnections[id];
        this.__removeConnection(info);
        info.conn.close();
        break;
      }
    }
  }

  private async __withConnection<T>(ctx: IContext, f: (conn: ILLDBConnection) => Promise<T>): Promise<T> {
    const conn = await this.__popConnection(ctx);
    try {
      return await f(conn);
    } finally {
      this.__returnConnection(conn);
    }
  }
}

class Semaphore {
  private __waiters: (() => void)[] = [];
  private __closed: boolean = false;

  constructor() {
  }

  close() {
    this.__closed = true;
    this.signal();
  }

  signal() {
    for (const w of this.__waiters)
      w();
    this.__waiters = [];
  }

  async wait(): Promise<void> {
    if (this.__closed)
      return;

    return new Promise<void>((resolve, reject) => {
      this.__waiters.push(resolve);
    });
  }
}

export function createPool(options: IPoolOptions): IDB {
  return new DBPool(options);
}
