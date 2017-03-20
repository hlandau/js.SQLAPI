import {createPool} from "hlandau.SQLAPI/Pool";
import "hlandau.SQLAPI-Pg/LLDB_Pg";
import {background} from "hlandau.Context";
import * as chai from "chai";

chai.should();

describe('Pool', () => {
  it('should function', async () => {
    const pool = createPool({
      driverName: 'Pg',
      driverArgs: {
        connectionSpec: '',
      },
    });

    const rows = await pool.query(background(), `SELECT '42'::text`, []);
    let got = false;
    for await (const row of rows as any) {
      got.should.equal(false);
      row[0].should.equal('42');
      got = true;
    }

    await pool.close();
  });
});
