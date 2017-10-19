import mongoose from 'mongoose';

const taskSchema = mongoose.Schema({
  name: String,
  description: String,
  hardDueDate: Date,
  softDueDate: Date,
  priority: String,
}, { discriminatorKey: 'type' });

export const Task = mongoose.model('Task', taskSchema);


const assetTaskSchema = mongoose.Schema({
  assets: [String],
  conversation: String,
});

export const AssetTask = Task.discriminator('AssetTask', assetTaskSchema);



export class MutableAssetTask extends AssetTask {

  task = this;

  async edit({ input }) {
    await Object.assign(this, input).save();
    return true;
  }

  async delete() {
    await this.remove();
    return true;
  }
}
