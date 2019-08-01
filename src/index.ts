import * as assert from 'assert';
import Axios, { AxiosInstance } from 'axios';
import { ObjectID, ObjectId } from 'bson';
import Erz, { forbidden, internal } from 'erz';
import { EventEmitter } from 'events';
import { decode, sign, verify } from 'jsonwebtoken';
import { Middleware } from 'koa';
import { ClientSession, Connection, Model } from 'mongoose';
import * as querystring from 'querystring';
import { IQueueStatResult } from './index';
import messageSchema, { IMessageDocument } from './schema';

export interface IMSInputOptions {
  service: number;
  secret: string;
}

export class MSInput {
  private _service: number;
  private _secret: string;

  constructor(options: IMSInputOptions) {
    this._service = options.service;
    this._secret = options.secret;
  }

  public check(serviceToken: string) {
    assert(serviceToken, forbidden('Service token is required.'));
    const tokenPayload = (() => {
      try {
        return verify(serviceToken, this._secret) as {
          from: number;
          to: number;
        };
      } catch (e) {
        throw forbidden('Invalid service token.');
      }
    })();
    assert(tokenPayload.to === this._service, forbidden('The token cannot access this service.'));
  }

  public middleware(): Middleware {
    return async (ctx, next) => {
      this.check(ctx.get('MSIO-service-token'));
      await next();
    };
  }
}

export interface IMSOutputOptions {
  service: number;
  secret: string;

  getServiceToken: F_GetServiceToken;
}

function isOverHalf(serviceToken: string) {
  const payload = decode(serviceToken) as { iat: number, exp: number };
  const now = Date.now() / 1000;
  return now > (payload.iat + payload.exp) / 2;
}

export class MSOutput {
  public static serviceTokens: { [to: number]: string } = {};

  private _options: IMSOutputOptions;

  constructor(options: IMSOutputOptions) {
    this._options = options;
  }

  public async getServiceToken(destination: number) {
    const serviceToken = MSOutput.serviceTokens[destination];
    if (!serviceToken || isOverHalf(serviceToken)) {
      const exchangeToken = sign({
        from: this._options.service,
        to: destination,
      }, this._options.secret);
      MSOutput.serviceTokens[destination] = await this._options.getServiceToken(this._options.service, destination, exchangeToken);
    }
    return MSOutput.serviceTokens[destination];
  }
}

export interface IMSDestinationOptions {
  service: number;
  baseURL: string;
  output: MSOutput;
  pulseInterval: number;
}

export interface IParams {
  [key: string]: any;
}

export interface IBody {
  [key: string]: any;
}

export class MSDestination {
  public service: number;

  public get isHealthy() {
    return this._isHealthy;
  }

  private _instance: AxiosInstance;
  private _isHealthy: boolean = true;
  private _pulseTimer: NodeJS.Timer;

  constructor({
    service,
    baseURL,
    output,
    pulseInterval,
  }: IMSDestinationOptions) {
    this._instance = Axios.create({
      baseURL,
      paramsSerializer(params) {
        if (!params) {
          return params;
        }
        Object.keys(params).forEach(key => {
          const value = params[key];
          if (typeof value === 'object') {
            params[key] = JSON.stringify(value);
          }
        });
        return querystring.stringify(params);
      },
    });
    this._instance.interceptors.request.use(async config => {
      config.headers['MSIO-service-token'] = await output.getServiceToken(service);
      return config;
    });
    this._instance.interceptors.response.use(response => {
      this._isHealthy = true;
      return response;
    }, error => {
      if (!error.response) {
        this._isHealthy = false;
        throw error;
      } else {
        this._isHealthy = error.response.status < 502;
        throw new Erz(decodeURI(error.response.headers.error || error.response.data), error.response.status);
      }
    });

    this._pulseTimer = setInterval(this._pulse.bind(this), pulseInterval);
    this._pulse();

    this.service = service;
  }

  public async read<T>(path: string, params: IParams) {
    const { data } = await this._instance.get<T>(path, {
      params,
    });
    return data;
  }

  public async write<T>(path: string, body: IBody) {
    const { data } = await this._instance.put<T>(path, body);
    return data;
  }

  public async writeRead<T>(path: string, body: IBody) {
    const { data } = await this._instance.post<T>(path, body);
    return data;
  }

  public destroy() {
    clearInterval(this._pulseTimer);
  }

  private _pulse() {
    this._instance.head('/').then(() => {
      this._isHealthy = true;
    }).catch((e: Error) => {
      this._isHealthy = false;
    });
  }
}

export interface IMSQueueDelegate {
  didAddMessage?(message: IMessageDocument, session?: ClientSession): Promise<void>;

  didDelayConsume?(message: IMessageDocument): Promise<void>;

  didConsumeSucceeded?(message: IMessageDocument, result: any): Promise<void>;

  didConsumeFailed?(message: IMessageDocument, error: Error): Promise<void>;

  didMessageDead?(message: IMessageDocument): Promise<void>;
}

interface IMSQueueForceDelegate {
  didAddMessage(message: IMessageDocument, session?: ClientSession): Promise<void>;

  didDelayConsume(message: IMessageDocument): Promise<void>;

  didConsumeSucceeded(message: IMessageDocument, result: any): Promise<void>;

  didConsumeFailed(message: IMessageDocument, error: Error): Promise<void>;

  didMessageDead(message: IMessageDocument): Promise<void>;
}

export interface IMSQueueOptionalOptions {
  connection?: Connection;

  seizeDuration?: number;
  idleDuration?: number;
  maxConsumeCount?: number;
  maxRetryTimes?: number;

  delegate?: IMSQueueDelegate;
}

export interface IMSQueueOptions {
  connection: Connection;

  seizeDuration: number;
  idleDuration: number;
  maxConsumeCount: number;
  maxRetryTimes: number;

  destination: MSDestination;

  delegate?: IMSQueueDelegate;
}

export interface IQueueStatResult {
  service: number;
  queued: number;
  seized: number;
  dead: number;
  detail: Array<{
    path: string;
    queued: number;
    seized: number;
    dead: number;
  }>;
}

export class MSQueue extends EventEmitter {
  private _Model: Model<IMessageDocument>;
  private _options: IMSQueueOptions;
  private _id: ObjectID;
  private _pendingMessages: ObjectID[] = [];
  private _timer?: NodeJS.Timer;
  private _delegate: IMSQueueForceDelegate;

  constructor(options: IMSQueueOptions) {
    super();
    try {
      this._Model = options.connection.model('msio-queue');
    } catch (e) {
      this._Model = options.connection.model('msio-queue', messageSchema);
    }
    this._options = options;
    this._delegate = {
      async didAddMessage(message: IMessageDocument, session?: ClientSession) {
        // noop.
      },
      async didConsumeFailed(message: IMessageDocument, error: Error) {
        // noop.
      },
      async didConsumeSucceeded(message: IMessageDocument, result: any) {
        // noop.
      },
      async didDelayConsume(message: IMessageDocument) {
        // noop.
      },
      async didMessageDead(message: IMessageDocument) {
        // noop.
      },
      ...(options.delegate || {}),
    };

    this._id = new ObjectId();

    this._start();
  }

  public async stat(): Promise<IQueueStatResult> {
    const result = await this._Model.aggregate([
      // Stage 1, find matched messages
      {
        $match: {
          service: this._options.destination.service,
        },
      },
      // Stage 2, select required fields
      {
        $project: {
          // specifications
          path: 1,
          isDead: 1,
          service: 1,
          seizedBy: 1,
        },
      },
      // Stage 3, group by path field
      {
        $group: {
          _id: '$path',
          queued: { $sum: 1 },
          dead: {
            $sum: {
              $cond: {
                if: { $eq: ['$isDead', true] },
                then: 1,
                else: 0,
              },
            },
          },
          seized: {
            $sum: {
              $cond: {
                if: { $ifNull: ['$seizedBy', true] },
                then: 0,
                else: 1,
              },
            },
          },
        },
      },
      // Stage 4, rename _id to path
      {
        $project: {
          // specifications
          path: '$_id',
          queued: 1,
          seized: 1,
          dead: 1,
        },
      },
      // Stage 5, group all
      {
        $group: {
          _id: this._options.destination.service,
          queued: { $sum: '$queued' },
          dead: { $sum: '$dead' },
          seized: { $sum: '$seized' },
          detail: { $push: '$$CURRENT' },
        },
      },
      // Stage 6, rename _id to service
      {
        $project: {
          // specifications
          service: '$_id',
          queued: 1,
          dead: 1,
          seized: 1,
          detail: 1,
        },
      },
    ])
      .allowDiskUse(true)
      .read('sp')
      .exec() as IQueueStatResult[];
    return result[0] || {
      service: this._options.destination.service,
      queued: 0,
      dead: 0,
      seized: 0,
      detail: [],
    };
  }

  public async addMessage({ session, ...message }: {
    path: string;
    body: IBody;
    depends?: ObjectID;
    producer: string;
    session?: ClientSession;
  }): Promise<ObjectID> {
    const msg = await new this._Model({
      ...message,
      service: this._options.destination.service,
      visibleAt: new Date(),
    }).save({ session });
    await this._delegate.didAddMessage(msg, session);
    return msg._id;
  }

  public destroy() {
    if (this._timer) {
      clearTimeout(this._timer);
    }
  }

  private _delayStart() {
    this._timer = setTimeout(() => {
      this._start();
    }, this._options.idleDuration);
  }

  private _start() {
    if (!this._options.destination.isHealthy) {
      this._delayStart();
      return;
    }
    (async () => {
      const messages = await this._Model.find({
        service: this._options.destination.service,
        isDead: false,
        visibleAt: { $lte: new Date() },
      }).sort({ _id: 1 }).limit(this._options.maxConsumeCount);

      const unorderedMessages: IMessageDocument[] = [];
      const orderedMessages: IMessageDocument[] = [];
      for (const message of messages) {
        const result = await this._Model.updateOne({
          _id: message._id,
          __v: message.__v,
        }, {
          $set: {
            seizedBy: this._id,
            visibleAt: new Date(Date.now() + this._options.seizeDuration),
          },
        });
        if (result.nModified !== 1) {
          continue;
        }
        this._pendingMessages.push(message._id);
        if (message.depends) {
          orderedMessages.push(message);
        } else {
          unorderedMessages.push(message);
        }
      }

      if (orderedMessages.length === 0 && unorderedMessages.length === 0) {
        this._delayStart();
        return;
      }

      await Promise.all(unorderedMessages.map(async message => {
        await this._consume(message);
      }));

      for (const message of orderedMessages) {
        await this._consume(message);
      }

    })().catch(e => {
      this.emit('error', e);
      this._delayStart();
    });
  }

  private async _consume(message: IMessageDocument) {
    try {
      if (message.depends) {
        const dependsMessage = await this._Model.findById(message.depends).read('primary');
        if (dependsMessage) {
          if (dependsMessage.isDead) {
            message.retryTimes = this._options.maxRetryTimes;
            throw Error('Depends message is dead.');
          }
          message.visibleAt = new Date(Date.now() + 500);
          message.seizedBy = undefined;
          await message.save();
          this._delegate.didDelayConsume(message).catch(e => this.emit('error', e));
          return;
        }
      }
      const result = await this._options.destination.write(message.path, message.body);
      await message.remove();
      this._delegate.didConsumeSucceeded(message, result).catch(e => this.emit('error', e));
    } catch (e) {
      message.visibleAt = new Date(Date.now() + 1000 * Math.pow(4, message.retryTimes));
      message.seizedBy = undefined;
      message.retryErrors.push(e.message);
      message.retryTimes += 1;
      if (message.retryTimes >= this._options.maxRetryTimes) {
        message.isDead = true;
      }
      message.save().catch((error: Error) => {
        this.emit('error', error);
      });
      this._delegate.didConsumeFailed(message, e).catch(err => this.emit('error', err));
      if (message.isDead) {
        this._delegate.didMessageDead(message).catch(err => this.emit('error', err));
      }
    } finally {
      this._pendingMessages.splice(this._pendingMessages.indexOf(message._id), 1);
      if (this._pendingMessages.length === 0) {
        this._start();
      }
    }
  }
}

export type F_GetServiceToken = (from: number, to: number, exchangeToken: string) => Promise<string>;

export interface IMSOptions {
  service: number;
  secret: string;

  connection?: Connection;

  seizeDuration?: number;
  idleDuration?: number;
  maxConsumeCount?: number;
  maxRetryTimes?: number;

  getServiceToken: F_GetServiceToken;
}

export default class MSIO extends EventEmitter {
  public get output() {
    return this._output;
  }

  public get input() {
    return this._input;
  }

  private _options: IMSOptions;
  private _output: MSOutput;
  private _input: MSInput;

  private _destinations: { [service: string]: MSDestination } = {};
  private _queues: { [service: string]: MSQueue } = {};

  constructor(options: IMSOptions) {
    super();
    this._options = options;
    const { service, secret, getServiceToken } = options;
    this._input = new MSInput({ service, secret });
    this._output = new MSOutput({ service, secret, getServiceToken });
  }

  public addDestination(service: number, baseURL: string, options?: IMSQueueOptionalOptions): void;
  public addDestination(service: number, baseURL: string, pulseInterval: number, options?: IMSQueueOptionalOptions): void;
  public addDestination(service: number, baseURL: string, pulseInterval: number | IMSQueueOptionalOptions = {}, options: IMSQueueOptionalOptions = {}) {
    if (typeof pulseInterval !== 'number') {
      options = pulseInterval;
      pulseInterval = 10000;
    }
    const existsDestination = this._destinations[service];
    if (existsDestination) {
      existsDestination.destroy();
      this._queues[service].destroy();
    }
    const destination = new MSDestination({
      service,
      baseURL,
      pulseInterval,
      output: this._output,
    });
    this._destinations[service] = destination;
    const {
      connection,
      seizeDuration = 10000,
      idleDuration = 3000,
      maxConsumeCount = 10,
      maxRetryTimes = 5,
      delegate,
    }: {
      service: number;
      connection?: Connection;
      idleDuration?: number;
      maxConsumeCount?: number;
      seizeDuration?: number;
      maxRetryTimes?: number;
      delegate?: IMSQueueDelegate;
    } = {
      ...this._options,
      ...options,
    };
    if (!connection) {
      throw new Error('connection parameter should be applied either in MSIO options or Queue options');
    }
    this._queues[service] = new MSQueue({
      connection,
      seizeDuration,
      idleDuration,
      maxConsumeCount,
      maxRetryTimes,
      destination,
      delegate,
    }).on('error', error => this.emit('error', error));
  }

  public destroy() {
    Object.keys(this._destinations).forEach(service => {
      this._destinations[service].destroy();
      this._queues[service].destroy();
    });
  }

  public getDestination(service: number) {
    const destination = this._destinations[service];
    assert(destination, internal(`Destination ${service} not found.`));
    return destination;
  }

  public getQueue(service: number) {
    const queue = this._queues[service];
    assert(queue, internal(`Queue ${service} not found`));
    return queue;
  }

  public async stat() {
    const destinations = await Promise.all(Object.keys(this._queues).map(async service => {
      const queue = this._queues[service];
      const statistic = await queue.stat();
      return {
        ...statistic,
        isHealthy: this._destinations[service].isHealthy,
      };
    }));
    const stat = destinations.reduce((memo, dest) => {
      memo.queued += dest.queued;
      memo.seized += dest.seized;
      memo.dead += dest.dead;
      return memo;
    }, { queued: 0, seized: 0, dead: 0 });
    return {
      ...stat,
      destinations,
    };
  }

  public async weakRead<T>(service: number, defaultValue: T, path: string, params: IParams): Promise<{
    data: T;
    isDefault: boolean;
    error?: Error;
  }> {
    const destination = this.getDestination(service);
    if (destination.isHealthy) {
      try {
        const result = await destination.read<T>(path, params);
        return {
          data: result,
          isDefault: false,
        };
      } catch (error) {
        return {
          data: defaultValue,
          isDefault: true,
          error,
        };
      }
    } else {
      return {
        data: defaultValue,
        isDefault: true,
      };
    }
  }

  public async read<T>(service: number, path: string, params: IParams): Promise<T> {
    const destination = this.getDestination(service);
    if (destination.isHealthy) {
      return await destination.read<T>(path, params);
    } else {
      throw internal(`Service [${service}] is not valid.`);
    }
  }

  public async write(service: number, path: string, body: IBody, producer: string, session?: ClientSession): Promise<ObjectID> {
    const queue = this.getQueue(service);
    return await queue.addMessage({
      path,
      body,
      producer,
      session,
    });
  }

  public async orderedWrite(service: number, depends: ObjectID, path: string, body: IBody, producer: string, session?: ClientSession): Promise<ObjectID> {
    const queue = this.getQueue(service);
    return await queue.addMessage({
      path,
      body,
      depends,
      producer,
      session,
    });
  }

  public async writeRead<T>(service: number, path: string, body: IBody): Promise<T> {
    const destination = this.getDestination(service);
    if (destination.isHealthy) {
      return await destination.writeRead<T>(path, body);
    } else {
      throw internal(`Service [${service}] is not valid.`);
    }
  }
}
