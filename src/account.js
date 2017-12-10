import mongoose from 'mongoose';
import bcrypt from 'bcrypt';
import zxcvbn from 'zxcvbn';
import { ServerSetting } from './server-setting';
import merge from 'lodash.merge';
import omit from 'lodash.omit';

const saltRounds = 10;



const accountSchema = mongoose.Schema({
  name: String,
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
      return this._permissions[domain];
    }
    return this._permissions;
  }

  async setPermission({ permission }, { user }) {
    await user.mustBeAbleTo('set account permissions', permission.domain);

    this._permissions[permission.domain] = permission.value;

    await this.save();
    return true;
  }

  async addPermission({ permission }, { user }) {
    await user.mustBeAbleTo('set account permissions', permission.domain);

    merge(this._permissions[permission.domain], permission.value);

    await this.save();
    return true;
  }

  async removePermissionPaths({ domain, paths }, { user }) {
    await user.mustBeAbleTo('set account permissions', domain);

    this._permissions[domain] = omit(this._permissions[domain], paths);

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
    _permissions: {
      auth: {
        uacct: {
          create: true,
          edit: true,
          view: true,
          delete: true
        },
        sacct: {
          create: true,
          edit: true,
          view: true,
          delete: true,
        },
        utoken: {
          revoke: true,
        },
        stoken: {
          create: true,
          revoke: true
        },
        pems: {
          view: true,
          set: true,
        }
      }
    }
  });
  
  await account.save();

  const setting = new ServerSetting({ AccountAdmin: { locked: true }});
  await setting.save();
}
