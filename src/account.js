import mongoose from 'mongoose';
import { AuthGrant } from './auth-grant';
import bcrypt from 'bcrypt';
import groupBy from 'lodash.groupby';
import zxcvbn from 'zxcvbn';

const saltRounds = 10;



const accountSchema = mongoose.Schema({
  name: String,
  _permissions: [{
    domain: String,
    value: String,
    default: [],
  }],
}, { discriminatorKey: 'type' });

class AccountClass {
  get query() { return this; }

  edit({ input }) {
    return Object.assign(this, input).save();
  }

  get pems() {
    return groupBy(this._permissions, pem => pem.domain);
  }

  async delete() {
    await this.remove();
    return true;
  }
}

accountSchema.loadClass(AccountClass);

export const Account = mongoose.model('Account', accountSchema);

const userAccountSchema = mongoose.Schema({
  email: { type: String, unique: true, sparse: true },
  password: {
    type: String,
    set(password) {
      const result = zxcvbn(password);
      if (result.score < 3) {
        throw new Error("Password is too weak");
      }
      // TODO: fix for async. Mongoose doesn't like promises here
      return bcrypt.hashSync(password, saltRounds);
    },
  }
});

class UserAccountClass {

  permissions({ domain }) {

    if (domain) {
      return this._permissions.filter(pem => pem.domain === domain);
    }
    return this._permissions;
  }

  async authGrant({ secretKeyPath }) {
    return new AuthGrant({ account: this, pems: this.pems, validFor: '1h', secretKeyPath });
  }

  authenticate(password) {
    return bcrypt.compare(password, this.password);
  }

  async grant({ permission }) {
    const existing = this._permissions.find(pem =>
      pem.domain === permission.domain && pem.value === permission.value
    );

    if (existing) {
      return false;
    }

    this._permissions.push(permission);
    await this.save();
    return true;
  }

  async revoke({ permission }) {
    const index = this._permissions.findIndex(pem =>
      pem.domain === permission.domain && pem.value === permission.value
    );

    if (index === -1) {
      return false;
    }

    this._permissions.splice(index, 1);
    await this.save();
    return true;
  }
}

userAccountSchema.loadClass(UserAccountClass);

export const UserAccount = Account.discriminator('UserAccount', userAccountSchema);
