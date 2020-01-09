import { ObjectID } from 'bson';
import { Document, Schema } from 'mongoose';
import { IBody } from './index';

const messageSchema = new Schema({
  service: { type: Number, required: true },
  path: { type: String, required: true },
  body: {},
  depends: { type: Schema.Types.ObjectId, required: false },
  retryTimes: { type: Number, required: true, default: 0 },
  retryErrors: [{ type: String, required: true }],
  isDead: { type: Boolean, required: true, default: false },
  producer: { type: String, required: true },
  visibleAt: { type: Date, required: true },
  seizedBy: { type: Schema.Types.ObjectId, required: false },
}, {
  read: 'primary',
  timestamps: true,
});

messageSchema.index({ service: 1, isDead: 1, visibleAt: 1 });
messageSchema.index({ seizedBy: 1 });

export interface IMessageDocument extends Document {
  service: number;
  path: string;
  body: IBody;
  depends?: ObjectID;
  retryTimes: number;
  retryErrors: string[];
  isDead: boolean;
  producer: string;
  visibleAt: Date;
  seizedBy?: ObjectID;
}

export default messageSchema;
