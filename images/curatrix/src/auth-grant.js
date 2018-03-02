import { RefreshToken } from './refresh-token';
import jwt from 'jsonwebtoken';
import fs from 'fs-extra';

export class AuthGrant {

  constructor({ account, pems, validFor, secretKeyPath, type }) {
    this.account = account;
    this.payload = { aid: account?.id, pems };
    this.validFor = validFor;
    this.secretKeyPath = secretKeyPath;
    this.type = type;
  }

  mutableAccount() {
    return this.account;
  }

  async queryToken() {
    const secretKey = await fs.readFile(this.secretKeyPath);
    return jwt.sign(this.payload, secretKey, { expiresIn: this.validFor, algorithm: 'ES384' });
  }

  async refreshToken({ expiration }) {

    const refreshToken = new RefreshToken({
      refreshToken: {
        expiration
      },
      queryToken: {
        payload: this.payload,
        validFor: this.validFor
      },
      type: this.type,
    });
    await refreshToken.save();
    return refreshToken.refreshToken.value;
  }
}
