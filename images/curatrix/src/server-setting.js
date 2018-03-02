import mongoose from 'mongoose';

export const ServerSetting = mongoose.model('ServerSetting', {
  AccountAdmin: {
    locked: Boolean
  }
});
