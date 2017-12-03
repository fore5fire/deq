import mongoose, { Schema } from 'mongoose';
import uuidv4 from 'uuid/v4';
import jwt from 'jsonwebtoken';
import fs from 'fs-extra';

const refreshTokenSchema = Schema({
  refreshToken: {
    value: { type: String, unique: true, default: uuidv4 },
    lastUsed: Date,
    expiration: Date
  },
  queryToken: {
    payload: { type: {}, required: true },
    validFor: String
  }
});

class RefreshTokenClass {

  gracePeriod = 60 * 5 * 1000; // 5 minute grace period for refreshing a token before it expires

  async refresh({ secretKeyPath }) {

    if (new Date() < this.lastUsed + this.expiration - this.gracePeriod) {
      log.warn("Client attempted to refresh token earlier than grace period before expiration! Possible sign of unathorized access to refresh token");
      throw new Error("To early to refresh token! It's possible someone else is using your refresh token - consider revoking it.");
    }

    const secretKey = await fs.readFile(secretKeyPath);
    return jwt.sign(this.queryToken.payload, secretKey, { expiresIn: this.queryToken.validFor, algorithm: 'ES384' });
  }

}

refreshTokenSchema.loadClass(RefreshTokenClass);

export const RefreshToken = mongoose.model('RefreshToken', refreshTokenSchema);
