import mongoose from 'mongoose';
import bcrypt from 'bcrypt';
import zxcvbn from 'zxcvbn';
import { ServerSetting } from './server-setting';
import merge from 'lodash.merge';
import omit from 'lodash.omit';
import map from 'lodash.map';

const saltRounds = 10;



const accountSchema = mongoose.Schema({
  _permissions: { type: {}, default: {} },
}, { discriminatorKey: 'type' });

class AccountClass {
  get query() { return this; }

  edit({ input }) {
    return Object.assign(this, input).save();
  }

  async permissions({ domain }, { user }) {
    user.mustBeAbleTo('view account permissions', { domain, aid: this.aid });

    if (domain) {
      return [{ domain, value: this._permissions[domain] }];
    }
    return map(this._permissions, (value, domain) => ({ value, domain }));
  }

  async setPermission({ input }, { user }) {
    await user.mustBeAbleTo('set account permissions', input.domain);

    const value = this._permissions[input.domain] = input.value;
    this.markModified('_permissions');

    await this.save();
    return {
      domain: input.domain,
      value,
    };
  }

  async addPermission({ input }, { user }) {
    await user.mustBeAbleTo('set account permissions', input.domain);

    if (!this._permissions[input.domain]) {
      this._permissions[input.domain] = {};
    }

    const value = merge(this._permissions[input.domain], input.value);
    this.markModified('_permissions');

    await this.save();
    return {
      domain: input.domain,
      value,
    };
  }

  async removePermissionPaths({ domain, paths }, { user }) {
    await user.mustBeAbleTo('set account permissions', domain);

    this._permissions[domain] = omit(this._permissions[domain], paths);
    this.markModified('_permissions');

    await this.save();
    return {
      domain,
      value: this._permissions[domain],
    };
  }

  async delete() {
    await this.remove();
    return true;
  }
}

accountSchema.loadClass(AccountClass);

export const Account = mongoose.model('Account', accountSchema);

const userAccountSchema = mongoose.Schema({
  names: {
    first: String,
    last: String,
  },
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
  },
});

class UserAccountClass {

  get name() {
    return `${this.names.first} ${this.names.last}`;
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
    names: {
      first: 'Bootstrap',
      last: 'Admin'
    },
    email,
    password,
    _permissions: {
      auth: {
        acct: {
          usr: {
            create: true,
            edit: true,
            view: true,
            delete: true
          },
          svc: {
            create: true,
            edit: true,
            view: true,
            delete: true,
          },
          pem: {
            view: true,
            edit: true
          }
        },
        tkn: {
          usr: {
            revoke: true,
          },
          svc: {
            create: true,
            revoke: true
          }
        },
      }
    }
  });

  await account.save();

  const setting = new ServerSetting({ AccountAdmin: { locked: true }});
  await setting.save();
}
