import Axios from 'axios';
import { createConnection } from 'mongoose';
import MSIO from '../src';

const keys = require('./keys.json');

const connection = createConnection(keys.mongodb);

const msio = new MSIO({
  service: 1,
  secret: keys.secret,
  connection,
  seizeDuration: 10000,
  idleDuration: 3000,
  maxConsumeCount: 10,
  maxRetryTimes: 5,

  async getServiceToken(from, to, exchangeToken) {
    const { data } = await Axios.post<{ serviceToken: string }>(
      'http://localhost:3002/auth/serviceToken',
      {
        exchangeToken,
        from,
      }, {
        params: {
          service: to,
        },
      },
    );
    return data.serviceToken;
  },
});

msio.addDestination(
  2,
  'http://localhost:3003/ms',
  10000,
);

const delay = (seconds: number = 1) => new Promise(resolve => setTimeout(resolve, 1000 * seconds));

describe('MSIO', function() {
  this.timeout(100000);

  it('weak read should ok', async () => {
    const result = await msio.weakRead(2, {
      original: '',
      x40: '',
      x64: '',
      x100: '',
      x280: '',
    }, '/user/avatar', { pid: '190102094337774001' });
    console.log(result);
  });

  it('read should ok', async () => {
    await delay();
    const result = await msio.read(2, '/user/avatar', { pid: '190102094337774001' });
    console.log(result);
  });

  it('write should ok', async () => {
    const result = await msio.write(2, '/', {
      foo: 'bar',
    }, __dirname);
    console.log(result);
  });

  it('cycle write', async () => {
    let depends = await msio.write(2, '/', {
      foo: 'first',
    }, __dirname);
    for (let i = 0; i < 100; i += 1) {
      depends = await msio.orderedWrite(
        2,
        depends,
        '/',
        { foo: String(i) },
        __dirname,
      );
    }
  });

  it.only('stat', async () => {
    const result = await msio.stat();
    console.log(JSON.stringify(result, null, 2));
  });
});

after(async function() {
  this.timeout(100000);
  await delay(20);
  msio.destroy();
  await connection.close();
});
