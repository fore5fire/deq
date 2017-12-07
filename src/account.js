import mongoose from 'mongoose';
import { AuthGrant } from './auth-grant';
import bcrypt from 'bcrypt';
import groupBy from 'lodash.groupby';
import zxcvbn from 'zxcvbn';
import { AuthError } from 'graphql-error-codes';
import { ServerSetting } from './server-setting';

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

  async grant({ permission }, context) {
    if (!context.user.pems.auth.editPems) {
      throw new AuthError("Permission Denied");
    }
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

  async revoke({ permission }, context) {
    if (!context.user.pems.auth.editPems) {
      throw new Error("Permission Denied");
    }
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
}

userAccountSchema.loadClass(UserAccountClass);

export const UserAccount = Account.discriminator('UserAccount', userAccountSchema);




export async function bootstrapAdmin(email, password) {

  log.info('Creating bootstrap admin');

  const locked = await ServerSetting.findOne({ AccountAdmin: { locked: true } });
  if (locked) {
    log.warn('Could not bootstrap admin because the functionality is locked');
    return;
  }

  const account = new UserAccount({
    name: 'Bootstrap Admin',
    email,
    password,
    _permissions: [{
      domain: 'auth',
      value: 'editAcct',
    },
    {
      domain: 'auth',
      value: 'viewAcct',
    },
    {
      domain: 'auth',
      value: 'editPems',
    }]
  });
  await account.save();

  const setting = new ServerSetting({ AccountAdmin: { locked: true }});
  await setting.save();
}
