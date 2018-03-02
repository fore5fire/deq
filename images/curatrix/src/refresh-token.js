import mongoose from 'mongoose';
import uuidv4 from 'uuid/v4';
import jwt from 'jsonwebtoken';
import fs from 'fs-extra';

const refreshTokenSchema = mongoose.Schema({
  refreshToken: {
    value: { type: String, unique: true, default: uuidv4 },
    lastUsed: Date,
    expiration: Date
  },
  queryToken: {
    payload: { type: {}, required: true },
    validFor: String
  },
  type: { type: String, index: true }
});

class RefreshTokenClass {


  async refresh({ secretKeyPath }) {
    const secretKey = await fs.readFile(secretKeyPath);
    return jwt.sign(this.queryToken.payload, secretKey, { expiresIn: this.queryToken.validFor, algorithm: 'ES384' });
  }

}

refreshTokenSchema.loadClass(RefreshTokenClass);

export const RefreshToken = mongoose.model('RefreshToken', refreshTokenSchema);
